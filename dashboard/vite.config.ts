import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '.', '')
  const apiPort = env.OBSERVABILITY_PORT || '8787'

  return {
    plugins: [react()],
    server: {
      port: 4173,
      proxy: {
        '/api': `http://localhost:${apiPort}`,
      },
    },
  }
})
