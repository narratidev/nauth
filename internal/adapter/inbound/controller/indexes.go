package controller

import (
	"github.com/WirelessCar/nauth/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FieldIndex is a field index that the reconcilers list by. SetupWithManager
// registers them with the manager; a client without a manager (cmd/nauth-file)
// registers them itself.
type FieldIndex struct {
	Object  client.Object
	Field   string
	Extract client.IndexerFunc
}

// FieldIndexes returns every field index the reconcilers depend on.
func FieldIndexes() []FieldIndex {
	return []FieldIndex{
		{Object: &v1alpha1.AccountExport{}, Field: exportAccountNameIndexKey, Extract: exportBySpecAccountNameIndexFunc},
		{Object: &v1alpha1.AccountImport{}, Field: importAccountNameIndexKey, Extract: byAccountNameIndexFunc},
		{Object: &v1alpha1.AccountImport{}, Field: importExportAccountRefIndexKey, Extract: byExportAccountRefIndexFunc},
	}
}
