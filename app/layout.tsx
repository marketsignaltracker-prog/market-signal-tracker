import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Zorva Labs | Where Ideas Ignite",
  description:
    "Zorva Labs builds apps, websites, and digital strategies that move your business forward. App development, web services, SEO & marketing.",
  icons: {
    icon: "/favicon.svg",
  },
  metadataBase: new URL("https://zorvalabs.com"),
  openGraph: {
    title: "Zorva Labs | Where Ideas Ignite",
    description:
      "We build apps, websites, and digital strategies that move your business forward. Beautiful design meets bold performance.",
    url: "https://zorvalabs.com",
    siteName: "Zorva Labs",
    locale: "en_US",
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
    title: "Zorva Labs | Where Ideas Ignite",
    description:
      "We build apps, websites, and digital strategies that move your business forward. Beautiful design meets bold performance.",
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
  viewportFit: "cover",
  themeColor: "#0a0a1a",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
