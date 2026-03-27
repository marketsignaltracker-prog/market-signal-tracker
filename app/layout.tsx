import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { AuthProvider } from "@/lib/auth-context";
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
  title: "Market Signal Tracker | Insider Buying & High-Signal Stocks",
  description:
    "Find high-signal stock opportunities using insider buying, SEC filings, earnings surprises, and momentum signals.",
  manifest: "/manifest.json",
  appleWebApp: {
    capable: true,
    statusBarStyle: "black-translucent",
    title: "Market Signal Tracker",
  },
  other: {
    "mobile-web-app-capable": "yes",
  },
  openGraph: {
    title: "Market Signal Tracker | Insider Buying & High-Signal Stocks",
    description:
      "Stop guessing. Follow the smart money. We surface 30-50 high-conviction stock picks daily using insider trades, congressional buys, and momentum signals.",
    url: "https://www.marketsignaltracker.com",
    siteName: "Market Signal Tracker",
    images: [
      {
        url: "https://www.marketsignaltracker.com/og-image.png",
        width: 1200,
        height: 630,
        alt: "Market Signal Tracker — Stop guessing. Follow the smart money.",
      },
    ],
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
    title: "Market Signal Tracker | Insider Buying & High-Signal Stocks",
    description:
      "Stop guessing. Follow the smart money. 30-50 high-conviction stock picks daily.",
    images: ["https://www.marketsignaltracker.com/og-image.png"],
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
  viewportFit: "cover",
  themeColor: "#080d18",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <link rel="apple-touch-icon" href="/apple-touch-icon.png" />
        <script
          dangerouslySetInnerHTML={{
            __html: `
              !function(f,b,e,v,n,t,s)
              {if(f.fbq)return;n=f.fbq=function(){n.callMethod?
              n.callMethod.apply(n,arguments):n.queue.push(arguments)};
              if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
              n.queue=[];t=b.createElement(e);t.async=!0;
              t.src=v;s=b.getElementsByTagName(e)[0];
              s.parentNode.insertBefore(t,s)}(window, document,'script',
              'https://connect.facebook.net/en_US/fbevents.js');
              fbq('init', '904015652453670');
              fbq('track', 'PageView');
            `,
          }}
        />
        <noscript>
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            height="1"
            width="1"
            style={{ display: "none" }}
            src="https://www.facebook.com/tr?id=904015652453670&ev=PageView&noscript=1"
            alt=""
          />
        </noscript>
      </head>
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <AuthProvider>{children}</AuthProvider>
        <script
          dangerouslySetInnerHTML={{
            __html: `
              if ("serviceWorker" in navigator) {
                window.addEventListener("load", () => {
                  navigator.serviceWorker.register("/sw.js").catch(() => {});
                });
              }
            `,
          }}
        />
      </body>
    </html>
  );
}