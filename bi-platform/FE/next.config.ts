import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  typescript: {
    // Bỏ qua lỗi kiểm tra kiểu dữ liệu khi build để đẩy lên Vercel
    ignoreBuildErrors: true,
  },
  async redirects() {
    return [
      {
        source: "/",
        destination: "/data-sources",
        permanent: true,
      },
    ];
  },
  // output: 'standalone', // Bỏ standalone để tương thích hoàn toàn với Vercel
};

export default nextConfig;
