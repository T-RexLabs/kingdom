package theme

import (
	"fmt"
	"math"
	"strconv"
)

type rgb [3]float64

func parseColor(s string) rgb {
	var c rgb
	for i := range c {
		n, _ := strconv.ParseUint(s[1+i*2:3+i*2], 16, 8)
		c[i] = float64(n)
	}
	return c
}
func mix(a, b string, amount float64) string {
	x, y := parseColor(a), parseColor(b)
	return fmt.Sprintf("#%02x%02x%02x", int(math.Round(x[0]+(y[0]-x[0])*amount)), int(math.Round(x[1]+(y[1]-x[1])*amount)), int(math.Round(x[2]+(y[2]-x[2])*amount)))
}
func luminance(s string) float64 {
	c := parseColor(s)
	for i := range c {
		c[i] /= 255
		if c[i] <= .04045 {
			c[i] /= 12.92
		} else {
			c[i] = math.Pow((c[i]+.055)/1.055, 2.4)
		}
	}
	return .2126*c[0] + .7152*c[1] + .0722*c[2]
}
func contrast(a, b string) float64 {
	x, y := luminance(a), luminance(b)
	if x < y {
		x, y = y, x
	}
	return (x + .05) / (y + .05)
}
func readable(color, background string, target float64) string {
	if contrast(color, background) >= target {
		return color
	}
	end := "#ffffff"
	if contrast("#000000", background) > contrast(end, background) {
		end = "#000000"
	}
	for i := 1; i <= 100; i++ {
		c := mix(color, end, float64(i)/100)
		if contrast(c, background) >= target {
			return c
		}
	}
	return end
}
func contrastText(background string) string {
	if contrast("#ffffff", background) > contrast("#000000", background) {
		return "#ffffff"
	}
	return "#000000"
}

// derivePalette is the sole palette algorithm. Clients preview the resolved
// server response rather than maintaining a second implementation.
func derivePalette(p map[string]string) map[string]string {
	canvas, surface, text, accent := p["canvas"], p["surface"], p["text"], p["accent"]
	t := map[string]string{
		"canvas": canvas, "surface.base": surface, "surface.inset": mix(surface, canvas, .6), "surface.elevated": mix(surface, text, .045), "surface.overlay": mix(surface, text, .07),
		"text.primary": text, "text.secondary": readable(mix(text, canvas, .22), surface, 6), "text.subtle": readable(mix(text, canvas, .35), surface, 6), "text.inverse": contrastText(text),
		"border.default": mix(surface, text, .22), "border.strong": readable(mix(surface, text, .5), surface, 4),
		"accent.base": accent, "accent.hover": readable(mix(accent, text, .12), contrastText(accent), 4.5), "accent.pressed": readable(mix(accent, canvas, .14), contrastText(accent), 4.5), "accent.foreground": contrastText(accent), "accent.subtle": mix(surface, accent, .12),
		"focus.ring": readable(accent, surface, 4), "focus.offset": surface, "selection.background": accent, "selection.foreground": contrastText(accent),
		"control.disabledBackground": mix(surface, canvas, .5), "control.disabledForeground": mix(text, surface, .5),
		"code.background": canvas, "code.foreground": text, "tool.background": mix(surface, canvas, .25), "tool.border": mix(surface, text, .22),
		"chart.axis": readable(mix(text, canvas, .22), surface, 6), "chart.grid": mix(surface, text, .15),
		"dag.edge": readable(mix(text, canvas, .38), surface, 3), "dag.edgeCompleted": readable(accent, surface, 3), "dag.edgeActive": readable(accent, surface, 3), "dag.nodeActiveBackground": mix(surface, accent, .10),
	}
	for state, c := range map[string]string{"neutral": "#7b8993", "running": "#438cef", "attention": "#d99325", "succeeded": "#28a271", "failed": "#dd555e", "cancelled": "#9970c5", "rework": "#c265a5"} {
		bg := mix(surface, c, .10)
		t["status."+state+".background"] = bg
		t["status."+state+".foreground"] = readable(c, bg, 4.5)
		t["status."+state+".border"] = readable(c, surface, 3)
	}
	for i, c := range []string{accent, "#438cef", "#28a271", "#9970c5", "#d99325", "#c265a5", "#39a6ab", "#dd555e"} {
		t[fmt.Sprintf("chart.category%d", i+1)] = readable(c, surface, 3)
	}
	return t
}
func validateContrast(m Manifest) error {
	t := m.Tokens
	var issues []Diagnostic
	check := func(fg, bg string, min float64) {
		ratio := contrast(t[fg], t[bg])
		if ratio+.001 < min {
			issues = append(issues, Diagnostic{"/tokens/" + fg, "contrast", fmt.Sprintf("Contrast against %s is %.2f:1; use at least %.1f:1 (suggested %s).", bg, ratio, min, readable(t[fg], t[bg], min))})
		}
	}
	for _, bg := range []string{"canvas", "surface.base", "surface.inset", "surface.elevated", "surface.overlay", "tool.background", "dag.nodeActiveBackground"} {
		for _, fg := range []string{"text.primary", "text.secondary", "text.subtle"} {
			check(fg, bg, 4.5)
		}
	}
	// Accent is also foreground text for links, navigation, selected tabs and
	// their hover states. Button-foreground contrast alone cannot protect those.
	for _, bg := range []string{"canvas", "surface.base", "surface.inset", "surface.elevated", "surface.overlay", "tool.background", "dag.nodeActiveBackground", "accent.subtle"} {
		check("accent.base", bg, 4.5)
		check("accent.hover", bg, 4.5)
	}
	for _, bg := range []string{"accent.base", "accent.hover", "accent.pressed"} {
		check("accent.foreground", bg, 4.5)
	}
	check("selection.foreground", "selection.background", 4.5)
	check("code.foreground", "code.background", 4.5)
	for _, bg := range []string{"canvas", "surface.base", "surface.overlay"} {
		check("focus.ring", bg, 3)
		check("border.strong", bg, 3)
	}
	for _, state := range []string{"neutral", "running", "attention", "succeeded", "failed", "cancelled", "rework"} {
		check("status."+state+".foreground", "status."+state+".background", 4.5)
		check("status."+state+".border", "surface.base", 3)
	}
	if len(issues) > 0 {
		return &ValidationError{issues}
	}
	return nil
}
