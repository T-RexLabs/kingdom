package theme

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/png"
	"testing"
)

func TestRasterBudgetsAndDeclaredType(t *testing.T) {
	r := resolverForTest(t)
	m := draftForTest()
	m["assets"] = map[string]any{"scene": Asset{Path: "assets/scene.png", MediaType: "image/png"}}
	m["artwork"] = map[string]string{"overview": "asset:scene"}
	var pngData bytes.Buffer
	if err := png.Encode(&pngData, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(rawForTest(t, m), map[string][]byte{"assets/scene.png": pngData.Bytes()}); err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{[]byte(safeSVG), pngData.Bytes()[:20]} {
		if _, err := r.Resolve(rawForTest(t, m), map[string][]byte{"assets/scene.png": data}); err == nil {
			t.Fatal("mislabeled or truncated image accepted")
		}
	}
	pngData.Reset()
	if err := png.Encode(&pngData, image.NewRGBA(image.Rect(0, 0, 8193, 1))); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(rawForTest(t, m), map[string][]byte{"assets/scene.png": pngData.Bytes()}); err == nil {
		t.Fatal("oversized image accepted")
	}
}
func TestPortableNamesAndFontExpansionBudget(t *testing.T) {
	r := resolverForTest(t)
	m := draftForTest()
	m["id"] = "con"
	if _, err := r.Resolve(rawForTest(t, m), nil); err == nil {
		t.Fatal("reserved package name accepted")
	}
	m = assetDraft("assets/COM1.svg")
	if _, err := r.Resolve(rawForTest(t, m), map[string][]byte{"assets/COM1.svg": []byte(safeSVG)}); err == nil {
		t.Fatal("reserved asset name accepted")
	}
	font := make([]byte, 64)
	copy(font, "wOF2")
	binary.BigEndian.PutUint32(font[4:8], 0x00010000)
	binary.BigEndian.PutUint32(font[8:12], uint32(len(font)))
	binary.BigEndian.PutUint16(font[12:14], 1)
	binary.BigEndian.PutUint32(font[16:20], MaxPackageBytes+1)
	binary.BigEndian.PutUint32(font[20:24], 16)
	if err := validateWOFF2(font); err == nil {
		t.Fatal("oversized font expansion accepted")
	}
}
func TestWebPRejectsAnimationAndMissingPadding(t *testing.T) {
	container := func(kind string, data []byte) []byte {
		raw := make([]byte, 20+len(data))
		copy(raw, "RIFF")
		binary.LittleEndian.PutUint32(raw[4:8], uint32(len(raw)-8))
		copy(raw[8:], "WEBP")
		copy(raw[12:], kind)
		binary.LittleEndian.PutUint32(raw[16:20], uint32(len(data)))
		copy(raw[20:], data)
		return raw
	}
	if err := validateWebP(container("ANIM", []byte{0, 0})); err == nil {
		t.Fatal("animated image accepted")
	}
	// The frame header is bounded, but its odd-length RIFF chunk needs padding.
	if err := validateWebP(container("VP8L", []byte{0x2f, 0, 0, 0, 0})); err == nil {
		t.Fatal("missing chunk padding accepted")
	}
}
