import Sidebar from "./Sidebar";
import "./globals.css";

export const metadata = {
  title: "Polaris",
  description: "London Transit realtime metrics",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <Sidebar />
        <main className="main">{children}</main>
      </body>
    </html>
  );
}
