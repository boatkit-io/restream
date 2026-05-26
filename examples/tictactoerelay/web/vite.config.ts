import { defineConfig } from 'vite'
import { standardDecorators } from '@boatkit-io/resub/vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [standardDecorators(), react()],
})
