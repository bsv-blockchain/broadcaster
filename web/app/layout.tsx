import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono, Outfit } from "next/font/google";
import type { ReactNode } from "react";
import { PointerGlow } from "@/components/pointer-glow";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { ThemeProvider } from "@/components/theme-provider";
import "./globals.css";
import { alternateTypes, site } from "@/lib/site";

/*
 * Every face is under the SIL Open Font License 1.1 (licence texts in
 * public/licenses/), so it can ship inside the open-source binary. next/font
 * downloads them at build time and self-hosts them in the export.
 */
const geistSans = Geist({ variable: "--font-body", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-mono-face", subsets: ["latin"] });

/** Outfit (variable), the display face: a geometric sans for headings. */
const display = Outfit({ variable: "--font-display-face", subsets: ["latin"], display: "swap" });

export const metadata: Metadata = {
  metadataBase: new URL(site.url),
  title: "BSV Broadcaster: transaction broadcast manager API",
  description: site.description,
  applicationName: site.name,
  authors: [{ name: site.association, url: site.parent }],
  creator: site.association,
  publisher: site.association,
  keywords: [
    "BSV",
    "Bitcoin SV",
    "transaction broadcast",
    "Teranode",
    "ARC",
    "Arcade",
    "merkle proof",
    "Server-Sent Events",
    "webhooks",
    "Chaintracks",
  ],
  // Explicitly indexable, including by AI crawlers; robots.ts says the same.
  robots: { index: true, follow: true },
  openGraph: {
    type: "website",
    siteName: site.name,
    locale: "en",
    url: "/",
    title: "BSV Broadcaster: transaction broadcast manager API",
    description: site.description,
  },
  twitter: {
    card: "summary",
    title: "BSV Broadcaster: transaction broadcast manager API",
    description: site.description,
  },
  icons: {
    icon: [
      { url: "/favicon-32.png", sizes: "32x32", type: "image/png" },
      { url: "/favicon-16.png", sizes: "16x16", type: "image/png" },
      { url: "/icon-192.png", sizes: "192x192", type: "image/png" },
    ],
    apple: [{ url: "/apple-icon.png", sizes: "180x180", type: "image/png" }],
  },
  // Machine-readable companions, announced in the head as well as at their well-known paths.
  alternates: { types: alternateTypes },
};

export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#F4F2F0" },
    { media: "(prefers-color-scheme: dark)", color: "#0F1230" },
  ],
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

export default function RootLayout({
  children,
}: Readonly<{ children: ReactNode }>): ReactNode {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} ${display.variable}`}
      data-scroll-behavior="smooth"
      suppressHydrationWarning
    >
      <body id="top" className="flex min-h-dvh flex-col">
        {/* Follows the OS until the header switch is pressed, then remembers. */}
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
          <a
            href="#main"
            className="sr-only focus:not-sr-only focus:fixed focus:top-3 focus:left-3 focus:z-60 focus:rounded-md focus:bg-primary focus:px-4 focus:py-2 focus:text-sm focus:text-primary-foreground"
          >
            Skip to content
          </a>
          <SiteHeader />
          <main id="main" className="flex-1">
            {children}
          </main>
          <SiteFooter />
          <PointerGlow />
        </ThemeProvider>
      </body>
    </html>
  );
}
