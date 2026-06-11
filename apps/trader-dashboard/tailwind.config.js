/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#101214",
        surface: "#171b1a",
        border: "#2a302e",
        "border-inner": "#242a28",
        text: "#f3f5f4",
        muted: "#9aa5a1",
        "muted-2": "#8a9896",
        link: "#9dd8f0",
        alert: "#ffd28a",
        fresh: { bg: "#23352e", text: "#8df0b0" },
        stale: { bg: "#3a2922", text: "#ffb086" },
        bullish: { bg: "#18321f", text: "#86efac" },
        bearish: { bg: "#3a1a1a", text: "#fca5a5" },
        neutral: { bg: "#1e2524", text: "#94a3b8" },
        research: { bg: "#131918", border: "#1e2a26", count: "#7dd3fc", "count-bg": "#1e2d3a" },
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "-apple-system", "BlinkMacSystemFont", '"Segoe UI"', "sans-serif"],
      },
    },
  },
  plugins: [],
};
