package theme

import (
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
)

var assetPathPattern = regexp.MustCompile(`^assets/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.(?:svg|png|jpg|jpeg|webp|avif|woff2)$`)

func validateAssets(m Manifest, files map[string][]byte) error {
	if len(files) > MaxAssets {
		return invalid("/assets", "size", "A package supports at most 64 assets.")
	}
	total := 0
	declared := map[string]bool{}
	for id, a := range m.Assets {
		field := "/assets/" + id
		if !assetPathPattern.MatchString(a.Path) || !portableAssetPath(a.Path) {
			return invalid(field+"/path", "path", "Use a package-relative assets path.")
		}
		if declared[a.Path] {
			return invalid(field+"/path", "duplicate_path", "Each asset must have a distinct path.")
		}
		declared[a.Path] = true
		raw, ok := files[a.Path]
		if !ok {
			return invalid(field, "missing_asset", "Asset file is missing.")
		}
		total += len(raw)
		if len(raw) == 0 || len(raw) > MaxAssetBytes || total > MaxPackageBytes {
			return invalid(field, "size", "Asset or package exceeds its resource budget.")
		}
		ext := path.Ext(a.Path)
		expected := map[string]string{".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp", ".avif": "image/avif", ".woff2": "font/woff2"}[ext]
		if a.MediaType != expected {
			return invalid(field+"/mediaType", "media_type", "Media type must match the asset extension.")
		}
		var err error
		switch a.MediaType {
		case "image/svg+xml":
			err = validateSVG(raw)
		case "image/png", "image/jpeg":
			var config image.Config
			var format string
			config, format, err = image.DecodeConfig(bytes.NewReader(raw))
			if err == nil && "image/"+format != a.MediaType {
				err = fmt.Errorf("image content does not match declared type")
			}
			if err == nil {
				err = validateDimensions(config.Width, config.Height)
			}
			if err == nil {
				_, _, err = image.Decode(bytes.NewReader(raw))
			}
		case "image/webp":
			err = validateWebP(raw)
		case "image/avif":
			err = validateAVIF(raw)
		case "font/woff2":
			err = validateWOFF2(raw)
		default:
			err = fmt.Errorf("unsupported media format")
		}
		if err != nil {
			return invalid(field, "unsafe_asset", err.Error())
		}
	}
	for name := range files {
		if !declared[name] {
			return invalid("/assets", "undeclared_asset", "Package contains an undeclared file: "+name)
		}
	}
	return nil
}
func validateDimensions(w, h int) error {
	if w < 1 || h < 1 || w > 8192 || h > 8192 || int64(w)*int64(h) > MaxImagePixels {
		return fmt.Errorf("image dimensions exceed the 16 megapixel or 8192-pixel edge limit")
	}
	return nil
}

var svgNumberList = regexp.MustCompile(`^[0-9eE+.,\s-]+$`)
var svgPathData = regexp.MustCompile(`^[MmZzLlHhVvCcSsQqTtAa0-9eE+.,\s-]+$`)
var svgTransform = regexp.MustCompile(`^(?:(?:matrix|translate|scale|rotate|skewX|skewY)\([0-9.eE ,+\-]+\)\s*)+$`)
var svgColor = regexp.MustCompile(`^(?:none|currentColor|#[0-9a-fA-F]{3}|#[0-9a-fA-F]{6}|#[0-9a-fA-F]{8})$`)

func validateSVG(raw []byte) error {
	d := xml.NewDecoder(bytes.NewReader(raw))
	depth, roots, elements := 0, 0, 0
	rootViewBox := false
	for {
		token, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("invalid SVG XML: %w", err)
		}
		switch t := token.(type) {
		case xml.Directive, xml.ProcInst:
			return fmt.Errorf("SVG declarations and processing instructions are not allowed")
		case xml.StartElement:
			depth++
			elements++
			if depth > 24 || elements > 4096 {
				return fmt.Errorf("SVG geometry exceeds its complexity budget")
			}
			if t.Name.Space != "" && t.Name.Space != "http://www.w3.org/2000/svg" {
				return fmt.Errorf("SVG foreign namespaces are not allowed")
			}
			if depth == 1 {
				roots++
				if roots != 1 || t.Name.Local != "svg" {
					return fmt.Errorf("expected one SVG root")
				}
			}
			if !strings.Contains(" svg g path rect circle ellipse line polyline polygon title desc ", " "+t.Name.Local+" ") {
				return fmt.Errorf("SVG element %s is not allowed", t.Name.Local)
			}
			if t.Name.Local == "svg" && depth != 1 {
				return fmt.Errorf("nested SVG roots are not allowed")
			}
			for _, a := range t.Attr {
				name, value := a.Name.Local, a.Value
				if name == "xmlns" && a.Name.Space == "" && value == "http://www.w3.org/2000/svg" {
					continue
				}
				if a.Name.Space != "" {
					return fmt.Errorf("SVG namespaced attributes are not allowed")
				}
				if len(value) > 65536 {
					return fmt.Errorf("SVG attribute exceeds its complexity budget")
				}
				ok := false
				switch name {
				case "d":
					ok = svgPathData.MatchString(value)
				case "viewBox":
					fields := strings.Fields(strings.ReplaceAll(value, ",", " "))
					ok = len(fields) == 4
					for _, field := range fields {
						v, e := strconv.ParseFloat(field, 64)
						ok = ok && e == nil && !math.IsInf(v, 0) && !math.IsNaN(v)
					}
					if ok {
						w, _ := strconv.ParseFloat(fields[2], 64)
						h, _ := strconv.ParseFloat(fields[3], 64)
						ok = w > 0 && h > 0
						if depth == 1 {
							rootViewBox = true
						}
					}
				case "width", "height", "x", "y", "x1", "y1", "x2", "y2", "cx", "cy", "r", "rx", "ry", "points", "stroke-width", "stroke-dasharray", "stroke-dashoffset":
					ok = svgNumberList.MatchString(value)
				case "opacity", "fill-opacity", "stroke-opacity":
					v, e := strconv.ParseFloat(value, 64)
					ok = e == nil && v >= 0 && v <= 1
				case "fill", "stroke":
					ok = svgColor.MatchString(value)
				case "transform":
					ok = svgTransform.MatchString(value)
				case "fill-rule", "clip-rule":
					ok = value == "nonzero" || value == "evenodd"
				case "stroke-linecap":
					ok = value == "butt" || value == "round" || value == "square"
				case "stroke-linejoin":
					ok = value == "miter" || value == "round" || value == "bevel"
				case "stroke-miterlimit":
					ok = svgNumberList.MatchString(value)
				}
				if !ok {
					return fmt.Errorf("SVG attribute %s is not allowed or has an unsafe value", name)
				}
			}
		case xml.EndElement:
			depth--
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(t)) != "" {
				return fmt.Errorf("unexpected text outside SVG")
			}
		}
	}
	if roots != 1 || depth != 0 || !rootViewBox {
		return fmt.Errorf("SVG needs a complete root with a numeric viewBox")
	}
	return nil
}

// WebP and AVIF are inspected as bounded image containers; browser decoders
// perform pixel decoding. They are served only with their validated image MIME.
func validateWebP(raw []byte) error {
	if len(raw) < 20 || string(raw[:4]) != "RIFF" || string(raw[8:12]) != "WEBP" || uint64(binary.LittleEndian.Uint32(raw[4:8]))+8 != uint64(len(raw)) {
		return fmt.Errorf("invalid WebP container")
	}
	found := false
	for i := 12; i < len(raw); {
		if i+8 > len(raw) {
			return fmt.Errorf("truncated WebP chunk")
		}
		n := int(binary.LittleEndian.Uint32(raw[i+4 : i+8]))
		if n < 0 || n > len(raw)-i-8 {
			return fmt.Errorf("invalid WebP chunk size")
		}
		data := raw[i+8 : i+8+n]
		var w, h int
		switch string(raw[i : i+4]) {
		case "VP8X":
			if len(data) != 10 {
				return fmt.Errorf("invalid WebP extended header")
			}
			if data[0]&2 != 0 {
				return fmt.Errorf("animated WebP is not supported")
			}
			w = 1 + int(data[4]) + (int(data[5]) << 8) + (int(data[6]) << 16)
			h = 1 + int(data[7]) + (int(data[8]) << 8) + (int(data[9]) << 16)
		case "VP8 ":
			if len(data) < 10 || !bytes.Equal(data[3:6], []byte{0x9d, 1, 0x2a}) {
				return fmt.Errorf("invalid WebP frame")
			}
			w = int(binary.LittleEndian.Uint16(data[6:8]) & 0x3fff)
			h = int(binary.LittleEndian.Uint16(data[8:10]) & 0x3fff)
			found = true
		case "VP8L":
			if len(data) < 5 || data[0] != 0x2f {
				return fmt.Errorf("invalid WebP lossless frame")
			}
			bits := binary.LittleEndian.Uint32(data[1:5])
			w = int(bits&0x3fff) + 1
			h = int((bits>>14)&0x3fff) + 1
			found = true
		case "ANIM", "ANMF":
			return fmt.Errorf("animated WebP is not supported")
		}
		if w != 0 {
			if err := validateDimensions(w, h); err != nil {
				return err
			}
		}
		if i+8+n+(n%2) > len(raw) {
			return fmt.Errorf("missing WebP chunk padding")
		}
		i += 8 + n + (n % 2)
	}
	if !found {
		return fmt.Errorf("WebP has no image frame")
	}
	return nil
}
func validateAVIF(raw []byte) error {
	brand, dimensions, media := false, false, false
	var walk func([]byte, int) error
	walk = func(data []byte, depth int) error {
		if depth > 8 {
			return fmt.Errorf("AVIF container nesting exceeds its budget")
		}
		for len(data) > 0 {
			if len(data) < 8 {
				return fmt.Errorf("truncated AVIF box")
			}
			n := uint64(binary.BigEndian.Uint32(data[:4]))
			header := 8
			if n == 1 {
				if len(data) < 16 {
					return fmt.Errorf("truncated extended AVIF box")
				}
				n = binary.BigEndian.Uint64(data[8:16])
				header = 16
			}
			if n < uint64(header) || n > uint64(len(data)) {
				return fmt.Errorf("invalid AVIF box size")
			}
			kind := string(data[4:8])
			body := data[header:int(n)]
			switch kind {
			case "ftyp":
				if depth != 0 || len(body) < 8 || len(body)%4 != 0 {
					return fmt.Errorf("invalid AVIF file type")
				}
				for i := 0; i < len(body); i += 4 {
					if i == 4 {
						continue
					}
					if string(body[i:i+4]) == "avif" {
						brand = true
					}
					if string(body[i:i+4]) == "avis" {
						return fmt.Errorf("animated AVIF is not supported")
					}
				}
			case "meta":
				if len(body) < 4 {
					return fmt.Errorf("invalid AVIF metadata")
				}
				if err := walk(body[4:], depth+1); err != nil {
					return err
				}
			case "iprp", "ipco":
				if err := walk(body, depth+1); err != nil {
					return err
				}
			case "ispe":
				if len(body) != 12 {
					return fmt.Errorf("invalid AVIF dimensions")
				}
				if err := validateDimensions(int(binary.BigEndian.Uint32(body[4:8])), int(binary.BigEndian.Uint32(body[8:12]))); err != nil {
					return err
				}
				dimensions = true
			case "mdat":
				media = len(body) > 0
			}
			data = data[int(n):]
		}
		return nil
	}
	if err := walk(raw, 0); err != nil {
		return err
	}
	if !brand || !dimensions || !media {
		return fmt.Errorf("AVIF requires image branding, dimensions and media data")
	}
	return nil
}
func validateWOFF2(raw []byte) error {
	if len(raw) < 48 || string(raw[:4]) != "wOF2" || int(binary.BigEndian.Uint32(raw[8:12])) != len(raw) {
		return fmt.Errorf("invalid WOFF2 header")
	}
	flavor := binary.BigEndian.Uint32(raw[4:8])
	tables := binary.BigEndian.Uint16(raw[12:14])
	expanded := binary.BigEndian.Uint32(raw[16:20])
	compressed := binary.BigEndian.Uint32(raw[20:24])
	if (flavor != 0x00010000 && flavor != 0x4f54544f) || tables == 0 || tables > 256 || binary.BigEndian.Uint16(raw[14:16]) != 0 || expanded < 48 || expanded > MaxPackageBytes || compressed == 0 || int(compressed) > len(raw)-48 {
		return fmt.Errorf("unsupported or oversized WOFF2 font")
	}
	for _, start := range []int{28, 40} {
		offset := uint64(binary.BigEndian.Uint32(raw[start : start+4]))
		size := uint64(binary.BigEndian.Uint32(raw[start+4 : start+8]))
		if offset == 0 && size == 0 {
			continue
		}
		if offset < 48 || offset+size > uint64(len(raw)) {
			return fmt.Errorf("invalid WOFF2 metadata range")
		}
	}
	return nil
}

func portableName(name string) bool {
	stem := strings.ToUpper(strings.SplitN(name, ".", 2)[0])
	if stem == "CON" || stem == "PRN" || stem == "AUX" || stem == "NUL" {
		return false
	}
	if len(stem) == 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) && stem[3] >= '1' && stem[3] <= '9' {
		return false
	}
	return true
}
func portableAssetPath(name string) bool {
	for _, part := range strings.Split(name, "/") {
		if !portableName(part) {
			return false
		}
	}
	return true
}
