import { useState } from "react";
import { imageAssetURL } from "./assets";
import { useRoleIdentity, useTheme, useThemeCopy } from "./context";
import type { ArtworkSlot, CopySlot } from "./contract";
import { ThemeIcon } from "./theme-icon";

export function ThemeCopy({ slot }: { slot: CopySlot }) { return <>{useThemeCopy(slot)}</>; }
export function RoleIdentity({ role, showResponsibility = true, className = "" }: { role: string; showResponsibility?: boolean; className?: string }) {
  const identity = useRoleIdentity(role);
  return <span className={`theme-role-identity ${className}`}><ThemeIcon slot={`role.${role}`} /><span><span className="theme-role-name">{identity.name}</span>{showResponsibility && identity.name !== identity.canonicalName && <span className="theme-role-responsibility">{identity.canonicalName}</span>}</span></span>;
}
function OptionalImage({ src, alt, className }: { src: string; alt: string; className?: string }) {
  const [failed, setFailed] = useState(false);
  return failed ? null : <img src={src} alt={alt} className={className} onError={() => setFailed(true)} />;
}
export function ThemeArtwork({ slot, alt = "", className }: { slot: ArtworkSlot; alt?: string; className?: string }) {
  const { theme, assetURLs } = useTheme();
  const src = imageAssetURL(theme, assetURLs, theme.artwork[slot]);
  return src ? <OptionalImage key={src} src={src} alt={alt} className={className} /> : null;
}
export function RolePortrait({ role, className }: { role: string; className?: string }) {
  const { theme, assetURLs } = useTheme();
  const identity = useRoleIdentity(role);
  const src = imageAssetURL(theme, assetURLs, identity.portrait);
  return src ? <OptionalImage key={src} src={src} alt="" className={className} /> : null;
}
