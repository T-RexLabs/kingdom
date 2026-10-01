import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { applyBrowserBrand } from "./browser-brand";

class BrandImage {
  static instances: BrandImage[] = [];
  src = "";
  naturalWidth = 200;
  naturalHeight = 100;
  onload: (() => void) | null = null;
  onerror: (() => void) | null = null;
  constructor() { BrandImage.instances.push(this); }
}

describe("browser brand rendering", () => {
  let icon: HTMLLinkElement;
  let context: { fillStyle: string; fillRect: ReturnType<typeof vi.fn>; drawImage: ReturnType<typeof vi.fn> };
  const cleanups: (() => void)[] = [];
  function apply(assetURL: string | undefined, canvasColor = "#0d1110") {
    const cleanup = applyBrowserBrand(assetURL, canvasColor);
    cleanups.push(cleanup);
    return cleanup;
  }
  beforeEach(() => {
    BrandImage.instances = [];
    vi.stubGlobal("Image", BrandImage);
    context = { fillStyle: "", fillRect: vi.fn(), drawImage: vi.fn() };
    vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(context as unknown as CanvasRenderingContext2D);
    vi.spyOn(HTMLCanvasElement.prototype, "toDataURL").mockReturnValue("data:image/png;base64,rendered");
    icon = document.createElement("link");
    icon.rel = "icon";
    icon.setAttribute("href", "/favicon.svg");
    icon.setAttribute("type", "image/svg+xml");
    document.head.append(icon);
  });
  afterEach(() => {
    cleanups.splice(0).reverse().forEach((cleanup) => cleanup());
    icon.remove();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("keeps the fallback until a contrast-backed PNG is ready and preserves artwork proportions", () => {
    const restore = apply("/brand.svg");
    expect(icon.getAttribute("href")).toBe("/favicon.svg");
    const image = BrandImage.instances[0];
    expect(image.src).toBe(new URL("/brand.svg", location.href).href);
    image.onload?.();
    expect(context.fillStyle).toBe("#0d1110");
    expect(context.fillRect).toHaveBeenCalledWith(0, 0, 64, 64);
    expect(context.drawImage).toHaveBeenCalledWith(image, expect.closeTo(4), expect.closeTo(18), expect.closeTo(56), expect.closeTo(28));
    expect(HTMLCanvasElement.prototype.toDataURL).toHaveBeenCalledWith("image/png");
    expect(icon.getAttribute("href")).toBe("data:image/png;base64,rendered");
    expect(icon.getAttribute("type")).toBe("image/png");
    restore();
    expect(icon.getAttribute("href")).toBe("/favicon.svg");
    expect(icon.getAttribute("type")).toBe("image/svg+xml");
  });

  it("renders the selected custom brand and canvas color", () => {
    apply("/api/v1/themes/custom/revisions/revision/assets/brand", "#faf5e6");
    const image = BrandImage.instances[0];
    image.naturalWidth = 100;
    image.naturalHeight = 200;
    image.onload?.();
    expect(context.fillStyle).toBe("#faf5e6");
    expect(context.drawImage).toHaveBeenCalledWith(image, expect.closeTo(18), expect.closeTo(4), expect.closeTo(28), expect.closeTo(56));
  });

  it.each([`blob:${location.origin}/private-preview`, "data:image/svg+xml,%3Csvg/%3E"])("accepts locally created preview artwork: %s", (url) => {
    apply(url);
    expect(BrandImage.instances[0].src).toBe(url);
  });

  it.each(["https://outside.invalid/brand.svg", "//outside.invalid/brand.svg", "blob:https://outside.invalid/preview", "data:text/html,hello", "javascript:alert(1)", undefined])("does not load an unsupported or missing asset: %s", (url) => {
    apply(url);
    expect(BrandImage.instances).toHaveLength(0);
    expect(icon.getAttribute("href")).toBe("/favicon.svg");
  });

  it("keeps the fallback on image or canvas failure", () => {
    apply("/broken.svg");
    BrandImage.instances[0].onerror?.();
    expect(icon.getAttribute("href")).toBe("/favicon.svg");
    apply("/tainted.svg");
    context.drawImage.mockImplementation(() => { throw new Error("Unable to decode image"); });
    expect(() => BrandImage.instances[1].onload?.()).not.toThrow();
    expect(icon.getAttribute("href")).toBe("/favicon.svg");
    expect(icon.getAttribute("type")).toBe("image/svg+xml");
  });

  it("cancels a pending image and ignores an already queued load callback", () => {
    const restore = apply("/brand.svg");
    const image = BrandImage.instances[0];
    const queuedLoad = image.onload;
    restore();
    expect(image.onload).toBeNull();
    expect(image.onerror).toBeNull();
    queuedLoad?.();
    expect(context.drawImage).not.toHaveBeenCalled();
    expect(icon.getAttribute("href")).toBe("/favicon.svg");
  });

  it("never lets an older load or cleanup overwrite a newer presentation", () => {
    const restoreOld = apply("/old.svg");
    const oldLoad = BrandImage.instances[0].onload;
    apply("/new.svg");
    BrandImage.instances[1].onload?.();
    oldLoad?.();
    restoreOld();
    expect(context.drawImage).toHaveBeenCalledTimes(1);
    expect(context.drawImage).toHaveBeenCalledWith(BrandImage.instances[1], expect.closeTo(4), expect.closeTo(18), expect.closeTo(56), expect.closeTo(28));
    expect(icon.getAttribute("href")).toBe("data:image/png;base64,rendered");
  });

  it("restores absent favicon attributes and tolerates a missing favicon element", () => {
    icon.removeAttribute("href");
    icon.removeAttribute("type");
    const restore = apply("/brand.svg");
    BrandImage.instances[0].onload?.();
    restore();
    expect(icon).not.toHaveAttribute("href");
    expect(icon).not.toHaveAttribute("type");
    icon.remove();
    apply("/brand.svg");
    expect(BrandImage.instances).toHaveLength(1);
  });
});
