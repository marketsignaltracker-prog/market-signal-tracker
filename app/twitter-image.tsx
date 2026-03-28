import { ImageResponse } from "next/og";

export const alt = "Zorva Labs - Where Ideas Ignite";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default function Image() {
  return new ImageResponse(
    (
      <div
        style={{
          background: "linear-gradient(135deg, #0a0a1a 0%, #1a1a3e 50%, #0a0a1a 100%)",
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          fontFamily: "system-ui, sans-serif",
          position: "relative",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            position: "absolute",
            top: "-100px",
            right: "-100px",
            width: "500px",
            height: "500px",
            borderRadius: "50%",
            background: "radial-gradient(circle, rgba(99,102,241,0.3) 0%, transparent 70%)",
            display: "flex",
          }}
        />
        <div
          style={{
            position: "absolute",
            bottom: "-150px",
            left: "-100px",
            width: "600px",
            height: "600px",
            borderRadius: "50%",
            background: "radial-gradient(circle, rgba(168,85,247,0.2) 0%, transparent 70%)",
            display: "flex",
          }}
        />
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "16px",
            marginBottom: "24px",
          }}
        >
          <div
            style={{
              width: "64px",
              height: "64px",
              borderRadius: "16px",
              background: "linear-gradient(135deg, #6366f1, #a855f7)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: "32px",
              fontWeight: 800,
              color: "white",
            }}
          >
            Z
          </div>
          <span
            style={{
              fontSize: "52px",
              fontWeight: 800,
              color: "white",
              letterSpacing: "-1px",
            }}
          >
            Zorva Labs
          </span>
        </div>
        <div
          style={{
            fontSize: "28px",
            fontWeight: 500,
            color: "rgba(255,255,255,0.7)",
            marginBottom: "48px",
            letterSpacing: "4px",
            textTransform: "uppercase",
          }}
        >
          Where Ideas Ignite
        </div>
        <div
          style={{
            display: "flex",
            gap: "24px",
          }}
        >
          {["App Development", "Web Services", "SEO", "Digital Marketing"].map(
            (service) => (
              <div
                key={service}
                style={{
                  padding: "12px 28px",
                  borderRadius: "999px",
                  border: "1px solid rgba(99,102,241,0.4)",
                  background: "rgba(99,102,241,0.1)",
                  color: "rgba(255,255,255,0.85)",
                  fontSize: "18px",
                  fontWeight: 500,
                }}
              >
                {service}
              </div>
            )
          )}
        </div>
        <div
          style={{
            position: "absolute",
            bottom: "32px",
            fontSize: "18px",
            color: "rgba(255,255,255,0.4)",
            letterSpacing: "1px",
          }}
        >
          zorvalabs.com
        </div>
      </div>
    ),
    { ...size }
  );
}
