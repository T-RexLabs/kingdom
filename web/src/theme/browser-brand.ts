// Favicon contrast belongs to the App, independently of the browser's tab-strip
// color. Only validated local artwork reaches this renderer; it stays an image.
const owners = new WeakMap<HTMLLinkElement, symbol>();

function localImageURL(value: string | undefined): string | undefined {
  if (!value) return;
  try {
    const url = new URL(value, location.href);
    if (url.protocol === "data:" && value.startsWith("data:image/")) return url.href;
    if ((url.protocol === "blob:" || url.protocol === "http:" || url.protocol === "https:")
      && url.origin === location.origin && !url.username && !url.password) return url.href;
  } catch { /* An unavailable image leaves the static browser icon in place. */ }
}

export function applyBrowserBrand(assetURL: string | undefined, canvasColor: string): () => void {
  const icon = document.querySelector<HTMLLinkElement>('link[rel="icon"]');
  if (!icon) return () => {};
  const previous = { href: icon.getAttribute("href"), type: icon.getAttribute("type") };
  const owner = Symbol();
  owners.set(icon, owner);
  let disposed = false;
  let image: HTMLImageElement | undefined;
  const url = localImageURL(assetURL);
  if (url) {
    image = new Image();
    const source = image;
    source.onload = () => {
      if (disposed || owners.get(icon) !== owner || !icon.isConnected) return;
      try {
        if (!source.naturalWidth || !source.naturalHeight) return;
        const canvas = document.createElement("canvas");
        canvas.width = canvas.height = 64;
        const context = canvas.getContext("2d");
        if (!context) return;
        context.fillStyle = canvasColor;
        context.fillRect(0, 0, 64, 64);
        const scale = 56 / Math.max(source.naturalWidth, source.naturalHeight);
        const width = source.naturalWidth * scale;
        const height = source.naturalHeight * scale;
        context.drawImage(source, (64 - width) / 2, (64 - height) / 2, width, height);
        const png = canvas.toDataURL("image/png");
        if (!png.startsWith("data:image/png;")) return;
        icon.setAttribute("type", "image/png");
        icon.setAttribute("href", png);
      } catch { /* Decoding or canvas failure preserves the existing favicon. */ }
    };
    source.onerror = () => { source.onload = null; source.onerror = null; };
    source.src = url;
  }
  return () => {
    if (disposed) return;
    disposed = true;
    if (image) { image.onload = null; image.onerror = null; }
    if (owners.get(icon) !== owner) return;
    owners.delete(icon);
    for (const [attribute, value] of Object.entries(previous)) {
      if (value === null) icon.removeAttribute(attribute);
      else icon.setAttribute(attribute, value);
    }
  };
}
