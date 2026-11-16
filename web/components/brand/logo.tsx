import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The BSV Blockchain wordmark, as the organisation ships it.
 *
 * Taken from the mark in use on bsvblockchain.org rather than redrawn: the
 * previous mark here was a triangle nobody uses, and the wordmark beside it
 * was typeset in the display font, so the site was showing three different
 * logos depending on where you looked.
 *
 * It is a pure wordmark. There is no icon, no glyph and no monogram in the
 * official lockup, which is why `LogoMark` below sets the "BSV" half on its
 * own rather than inventing a symbol to stand in for one.
 *
 * The two brand colours are `#003FFF` and `#1B1EA9`, which resolve to this
 * site's `--brand-blue` and `--brand-navy` almost exactly (0.500 0.281 264 and
 * 0.364 0.209 269 against 0.51 0.28 264 and 0.33 0.19 268). They are set as
 * `--logo-bsv` and `--logo-blockchain` on the svg rather than hardcoded, so a
 * dark background or the neutral `/dpp` scope can override both halves without
 * touching the geometry.
 */
export function Logo({
  className,
  showWordmark = true,
}: {
  className?: string;
  /** False renders "BSV" alone, for a lockup too narrow for the full mark. */
  showWordmark?: boolean;
}): ReactNode {
  return showWordmark ? <LogoFull className={className} /> : <LogoMark className={className} />;
}

/**
 * The full lockup: "BSV BLOCKCHAIN", 224 x 15.
 *
 * `logo-tint` carries the two custom properties. On a dark panel or in the
 * footer, set them to `currentColor` and the whole mark follows the text.
 */
export function LogoFull({ className }: { className?: string }): ReactNode {
  return (
    <svg
      viewBox="0 0 224 15"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="BSV Blockchain"
      className={cn("logo-tint h-[0.9375rem] w-auto", className)}
    >
      {/* "BSV" — one path, the deep-blue half of the lockup. */}
      <path d="M13.7902 7.27336C15.0503 6.63307 15.7596 5.52661 15.7596 4.16352C15.7596 2.51193 14.915 0.542972 10.892 0.542972H0V14.6147H11.793C14.8355 14.6147 16.5805 13.1828 16.5805 10.6864C16.5805 9.02517 15.6195 7.85443 13.7908 7.27336H13.7902ZM3.12885 2.89661H10.1517C12.3917 2.89661 12.6507 3.90488 12.6507 4.66361C12.6507 5.87434 11.8098 6.48831 10.1517 6.48831H3.12885V2.89661ZM10.9526 12.2605H3.12885V8.57216H10.9526C12.0468 8.57216 13.3511 8.88851 13.3511 10.3969C13.3511 11.6683 12.5891 12.26 10.9526 12.26V12.2605ZM56.7954 0.542972L48.8879 14.6147H44.8581L36.9502 0.542972H40.3407L46.8928 12.2524L53.4248 0.542972H56.7954ZM35.7254 10.5512C35.7254 14.4937 31.2949 14.9994 27.4351 14.9994C24.499 14.9994 21.6087 14.6866 19.3418 14.1333V11.6415C19.6978 11.7539 20.0916 11.8592 20.5213 11.9563C22.4301 12.3881 25.015 12.6457 27.4351 12.6457C31.6519 12.6457 32.516 11.8394 32.516 10.6287C32.516 9.20081 31.5239 8.99328 27.2803 8.72654L26.4773 8.66884C20.7688 8.33325 18.7442 7.18478 18.7442 4.27943C18.7442 1.53251 21.3449 0.197266 26.6943 0.197266C29.5872 0.197266 32.6729 0.635093 34.9555 1.36852V3.83351C33.0352 2.91837 29.1807 2.5509 26.4741 2.5509C22.7097 2.5509 21.9536 3.21245 21.9536 4.27943C21.9536 5.62986 22.8724 6.00341 26.8701 6.27774L27.711 6.33545C32.6797 6.67812 35.7249 7.06533 35.7249 10.5517L35.7254 10.5512Z" fill="var(--logo-bsv)" />

      {/* "BLOCKCHAIN" — ten paths, one per letter. */}
      <g fill="var(--logo-blockchain)">
        <path d="M65.6529 14.5842V0.296875H71.3951C75.1052 0.296875 77.3358 1.78296 77.3358 4.22416C77.3358 5.85854 76.4301 6.96247 75.0168 7.47217C76.4964 7.87558 77.4901 8.78819 77.4901 10.5926C77.4901 13.3947 74.9062 14.5837 71.3503 14.5837H65.6523L65.6529 14.5842ZM68.8776 6.2412H71.5499C73.3165 6.2412 74.1116 5.64697 74.1116 4.47926C74.1116 3.41785 73.228 2.84488 71.572 2.84488H68.8776V6.2417V6.2412ZM68.8776 12.0367H71.5278C73.0959 12.0367 74.2664 11.6971 74.2664 10.3811C74.2664 9.17084 73.3607 8.78869 71.5499 8.78869H68.8776V12.0367Z" />
        <path d="M85.6388 11.7183H92.5731V14.5842H85.2628C83.2092 14.5842 82.4141 13.5653 82.4141 11.9304V0.296875H85.6388V11.7178V11.7183Z" />
        <path d="M95.334 7.45211C95.334 3.24898 98.6687 0.0429688 103.063 0.0429688C107.458 0.0429688 110.815 3.24847 110.815 7.45211C110.815 11.6558 107.458 14.8613 103.063 14.8613C98.6687 14.8613 95.334 11.677 95.334 7.45211ZM107.48 7.45211C107.48 4.92588 105.559 2.97261 103.063 2.97261C100.568 2.97261 98.6466 4.92588 98.6466 7.45211C98.6466 9.97835 100.59 11.9316 103.063 11.9316C105.537 11.9316 107.48 9.97835 107.48 7.45211Z" />
        <path d="M114.107 7.45138C114.107 3.31151 117.486 0.0214844 121.815 0.0214844C124.399 0.0214844 126.872 1.3375 127.976 3.56662L125.348 4.98892C124.619 3.75744 123.471 2.97239 121.682 2.97239C119.209 2.97239 117.42 4.86188 117.42 7.45189C117.42 10.0419 119.187 11.9314 121.682 11.9314C123.471 11.9314 124.663 11.1458 125.348 9.91485L127.976 11.3372C126.872 13.5663 124.398 14.8823 121.815 14.8823C117.486 14.8823 114.107 11.5918 114.107 7.45239V7.45138Z" />
        <path d="M132.04 14.5842V0.296875H135.264V6.04987H137.164C137.341 5.60394 137.627 5.15802 137.981 4.69134L141.58 0.296875H145.246L141.006 5.54017C140.409 6.30447 140.1 6.83492 140.1 7.36588C140.1 7.89684 140.409 8.40604 141.006 9.17033L145.379 14.5837H141.426L137.98 10.2317C137.627 9.78582 137.362 9.3399 137.185 8.89448H135.264V14.5837H132.039L132.04 14.5842Z" />
        <path d="M147.984 7.45138C147.984 3.31151 151.363 0.0214844 155.692 0.0214844C158.276 0.0214844 160.749 1.3375 161.853 3.56662L159.225 4.98892C158.496 3.75744 157.348 2.97239 155.559 2.97239C153.086 2.97239 151.296 4.86188 151.296 7.45189C151.296 10.0419 153.064 11.9314 155.559 11.9314C157.348 11.9314 158.54 11.1458 159.225 9.91485L161.853 11.3372C160.749 13.5663 158.275 14.8823 155.692 14.8823C151.363 14.8823 147.984 11.5918 147.984 7.45239V7.45138Z" />
        <path d="M165.918 14.5842V0.296875H169.143V6.04987H175.923V0.296875H179.147V14.5837H175.923V8.89448H169.143V14.5837H165.918V14.5842Z" />
        <path d="M187.052 2.31438C187.561 0.764525 188.885 0.0214844 190.277 0.0214844C191.668 0.0214844 193.015 0.764525 193.523 2.31438L197.454 14.5847H194.164L193.302 11.655L187.273 11.6763L186.412 14.5847H183.121L187.052 2.31438ZM188.112 8.89494H192.463L190.785 3.22698C190.697 2.92987 190.564 2.80232 190.299 2.80232C190.055 2.80232 189.901 2.92987 189.813 3.22698L188.112 8.89494H188.112Z" />
        <path d="M204.984 0.296875V14.5837H201.76V0.296875H204.984Z" />
        <path d="M210.551 14.5844V2.25038C210.551 0.828075 211.633 0 212.848 0C213.82 0 214.593 0.339632 215.078 1.12519L220.953 10.8055V0.297115H224.001V12.6312C224.001 14.0535 222.919 14.8603 221.748 14.8603C220.798 14.8603 219.981 14.5419 219.496 13.7564L213.621 4.01232V14.5844H210.551Z" />
      </g>
    </svg>
  );
}

/**
 * "BSV" on its own, 57 x 15, for a header that has run out of room and for
 * the square contexts a full wordmark cannot fill.
 */
export function LogoMark({ className }: { className?: string }): ReactNode {
  return (
    <svg
      viewBox="0 0 57 15"
      xmlns="http://www.w3.org/2000/svg"
      role="img"
      aria-label="BSV"
      className={cn("logo-tint h-[0.9375rem] w-auto", className)}
    >
      <path d="M13.7902 7.27336C15.0503 6.63307 15.7596 5.52661 15.7596 4.16352C15.7596 2.51193 14.915 0.542972 10.892 0.542972H0V14.6147H11.793C14.8355 14.6147 16.5805 13.1828 16.5805 10.6864C16.5805 9.02517 15.6195 7.85443 13.7908 7.27336H13.7902ZM3.12885 2.89661H10.1517C12.3917 2.89661 12.6507 3.90488 12.6507 4.66361C12.6507 5.87434 11.8098 6.48831 10.1517 6.48831H3.12885V2.89661ZM10.9526 12.2605H3.12885V8.57216H10.9526C12.0468 8.57216 13.3511 8.88851 13.3511 10.3969C13.3511 11.6683 12.5891 12.26 10.9526 12.26V12.2605ZM56.7954 0.542972L48.8879 14.6147H44.8581L36.9502 0.542972H40.3407L46.8928 12.2524L53.4248 0.542972H56.7954ZM35.7254 10.5512C35.7254 14.4937 31.2949 14.9994 27.4351 14.9994C24.499 14.9994 21.6087 14.6866 19.3418 14.1333V11.6415C19.6978 11.7539 20.0916 11.8592 20.5213 11.9563C22.4301 12.3881 25.015 12.6457 27.4351 12.6457C31.6519 12.6457 32.516 11.8394 32.516 10.6287C32.516 9.20081 31.5239 8.99328 27.2803 8.72654L26.4773 8.66884C20.7688 8.33325 18.7442 7.18478 18.7442 4.27943C18.7442 1.53251 21.3449 0.197266 26.6943 0.197266C29.5872 0.197266 32.6729 0.635093 34.9555 1.36852V3.83351C33.0352 2.91837 29.1807 2.5509 26.4741 2.5509C22.7097 2.5509 21.9536 3.21245 21.9536 4.27943C21.9536 5.62986 22.8724 6.00341 26.8701 6.27774L27.711 6.33545C32.6797 6.67812 35.7249 7.06533 35.7249 10.5517L35.7254 10.5512Z" fill="currentColor" />
    </svg>
  );
}

/**
 * The mark as a cube whose six faces all carry it.
 *
 * Hovering the logo turns it one full revolution on the X axis. A cube is the
 * one bit of decoration on this site that argues for itself: a block that
 * shows the same face whichever way it lands is the claim the whole site is
 * making about a base protocol that does not change under you.
 *
 * Six faces rather than the four an X turn can reveal, so the piece survives
 * being rotated on another axis later. The geometry is CSS (`.logo-cube` in
 * globals.css), which keeps the turn on the compositor and costs no
 * JavaScript.
 *
 * The faces are `/images/bsv-mark.svg`, which is the official "BSV" knocked
 * out of brand blue. They used to be a JPEG in a different blue with different
 * letterforms, so the cube and the wordmark beside it disagreed about what the
 * logo was.
 */
export function LogoCube({ className }: { className?: string }): ReactNode {
  return (
    <span aria-hidden className={cn("logo-cube", className)}>
      <span className="logo-cube-box">
        <span className="logo-cube-face logo-cube-front" />
        <span className="logo-cube-face logo-cube-back" />
        <span className="logo-cube-face logo-cube-right" />
        <span className="logo-cube-face logo-cube-left" />
        <span className="logo-cube-face logo-cube-top" />
        <span className="logo-cube-face logo-cube-bottom" />
      </span>
    </span>
  );
}
