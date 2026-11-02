/** @type {import('next').NextConfig} */
const nextConfig = {
  cacheComponents: true,
  partialPrefetching: true,
  // Metrics is the start page.
  async redirects() {
    return [{ source: "/", destination: "/metrics", permanent: false }];
  },
};

export default nextConfig;
