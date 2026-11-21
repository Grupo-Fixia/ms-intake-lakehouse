package schemas

import "embed"

// FS contiene los esquemas JSON de los contratos de eventos incrustados en el binario
// (ms-services, ms-payments y ms-users).
//
//go:embed *.json payments/*.json ms-users/*.json
var FS embed.FS

