package theme

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	gen "kingdom/internal/apigen"
)

func TestArchiveHTTPBudgetMatchesCanonicalPackageBudget(t *testing.T) {
	manifest := draftForTest()
	assets := map[string]any{}
	for i := 0; i < 4; i++ {
		assets[fmt.Sprintf("image%d", i)] = map[string]any{"path": fmt.Sprintf("assets/image%d.svg", i), "mediaType": "image/svg+xml"}
	}
	manifest["assets"] = assets
	prefix, suffix := `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10">`, `</svg>`
	data := []byte(prefix + strings.Repeat(" ", MaxAssetBytes-len(prefix)-len(suffix)) + suffix)
	var archive bytes.Buffer
	z := zip.NewWriter(&archive)
	add := func(name string, content []byte) {
		f, err := z.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	add("theme.json", rawForTest(t, manifest))
	for i := 0; i < 4; i++ {
		add(fmt.Sprintf("assets/image%d.svg", i), data)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := resolverForTest(t).ImportZIP(archive.Bytes()); err != nil {
		t.Fatalf("canonical valid archive rejected: %v", err)
	}
	schema, err := gen.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(archive.Bytes())
	for _, route := range []string{"/themes/import", "/themes/validate-archive"} {
		archiveSchema := schema.Paths.Find(route).Post.RequestBody.Value.Content["application/json"].Schema.Value.Properties["archive"].Value
		if archiveSchema.MaxLength == nil || *archiveSchema.MaxLength != uint64(base64.StdEncoding.EncodedLen(MaxArchiveBytes)) {
			t.Fatalf("%s archive budget diverges from canonical resolver", route)
		}
		if err := archiveSchema.VisitJSONString(encoded); err != nil {
			t.Errorf("%s rejects canonical valid archive: archive=%d base64=%d max=%d", route, archive.Len(), len(encoded), *archiveSchema.MaxLength)
		}
	}
}
