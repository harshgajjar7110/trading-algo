import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Survivor Trading Dashboard',
  description: 'Real-time dashboard for the Survivor options trading strategy',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-gray-50 min-h-screen">
        {children}
      </body>
    </html>
  );
}
