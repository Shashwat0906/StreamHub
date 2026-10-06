package dashboard

import (
	"embed"
	"io/fs"
)

// The compiled React app (web/ → `npm run build` → ui/dist). It is
// committed so `go build` works without Node.js.
//
//go:embed all:ui/dist
var uiFiles embed.FS

// UI returns the embedded web app.
func UI() fs.FS {
	sub, err := fs.Sub(uiFiles, "ui/dist")
	if err != nil {
		panic(err)
	}
	return sub
}
