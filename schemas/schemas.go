package schemas

import "embed"

// FS contiene los esquemas JSON de los contratos de eventos incrustados en el binario.
//
//go:embed *.json
var FS embed.FS

