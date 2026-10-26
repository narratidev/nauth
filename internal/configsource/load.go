package configsource

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// uidNamespace seeds the UIDs of loaded objects. A UID must not change across
// restarts: nauth compares it with the owner of the Secrets it wrote.
var uidNamespace = uuid.MustParse("6f1c2b9e-4f4e-4b8a-9a52-2d0f0c7b6e11")

// Snapshot is the desired state read from the directory.
type Snapshot struct {
	Objects []client.Object
	// Hash changes when any file in the directory changes.
	Hash string
	// Skipped counts documents of kinds this controller does not manage.
	Skipped int
}

// Load reads every .yaml, .yml and .json file at the top of dir. A file may
// hold several documents. Documents of other kinds are skipped, so nauth and
// nack can read the same ConfigMap. Every object lands in namespace; an
// object that names another namespace is an error.
func Load(dir, namespace string, scheme *runtime.Scheme, kinds []schema.GroupVersionKind) (*Snapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	managed := map[schema.GroupVersionKind]bool{}
	for _, gvk := range kinds {
		managed[gvk] = true
	}
	decoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()

	var names []string
	for _, e := range entries {
		// A ConfigMap volume keeps its data in ..data and timestamped
		// directories; the files at the top are symlinks into them.
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		switch filepath.Ext(e.Name()) {
		case ".yaml", ".yml", ".json":
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	snap := &Snapshot{}
	hash := sha256.New()
	seen := map[string]string{}
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		fmt.Fprintf(hash, "%s\x00%d\x00", name, len(raw))
		hash.Write(raw)

		docs := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
		for i := 0; ; i++ {
			var doc runtime.RawExtension
			if err := docs.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, fmt.Errorf("%s: document %d: %w", name, i, err)
			}
			if len(bytes.TrimSpace(doc.Raw)) == 0 || string(bytes.TrimSpace(doc.Raw)) == "null" {
				continue
			}
			var meta metav1.TypeMeta
			if err := utilyaml.Unmarshal(doc.Raw, &meta); err != nil {
				return nil, fmt.Errorf("%s: document %d: %w", name, i, err)
			}
			gvk := meta.GroupVersionKind()
			if !managed[gvk] {
				snap.Skipped++
				continue
			}
			decoded, _, err := decoder.Decode(doc.Raw, &gvk, nil)
			if err != nil {
				return nil, fmt.Errorf("%s: document %d: %w", name, i, err)
			}
			obj, ok := decoded.(client.Object)
			if !ok {
				return nil, fmt.Errorf("%s: document %d: %s is not an object", name, i, gvk)
			}
			if obj.GetName() == "" {
				return nil, fmt.Errorf("%s: document %d: %s has no name", name, i, gvk.Kind)
			}
			if ns := obj.GetNamespace(); ns != "" && ns != namespace {
				return nil, fmt.Errorf("%s: %s %s is in namespace %s, not %s", name, gvk.Kind, obj.GetName(), ns, namespace)
			}
			obj.SetNamespace(namespace)
			key := objectKey(gvk, obj.GetName())
			if prev, dup := seen[key]; dup {
				return nil, fmt.Errorf("%s %s is defined in both %s and %s", gvk.Kind, obj.GetName(), prev, name)
			}
			seen[key] = name
			obj.SetUID(stableUID(gvk, namespace, obj.GetName()))
			snap.Objects = append(snap.Objects, obj)
		}
	}
	snap.Hash = fmt.Sprintf("%x", hash.Sum(nil))
	return snap, nil
}

func objectKey(gvk schema.GroupVersionKind, name string) string {
	return gvk.GroupKind().String() + "/" + name
}

func stableUID(gvk schema.GroupVersionKind, namespace, name string) types.UID {
	return types.UID(uuid.NewSHA1(uidNamespace, []byte(gvk.GroupKind().String()+"/"+namespace+"/"+name)).String())
}
