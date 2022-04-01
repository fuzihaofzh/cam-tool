import fs from 'fs'
export default {
  // Global page headers: https://go.nuxtjs.dev/config-head
  head: {
    title: 'cam-tool',
    htmlAttrs: {
      lang: 'en'
    },
    meta: [
      { charset: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { hid: 'description', name: 'description', content: '' },
      { name: 'format-detection', content: 'telephone=no' }
    ],
    link: [
      { rel: 'icon', type: 'image/x-icon', href: '/favicon.ico' }
    ]
  },

  // Global CSS: https://go.nuxtjs.dev/config-css
  css: [
    'element-ui/lib/theme-chalk/index.css'
  ],

  // Plugins to run before rendering page: https://go.nuxtjs.dev/config-plugins
  plugins: [
    '@/plugins/element-ui'
  ],

  // Auto import components: https://go.nuxtjs.dev/config-components
  components: true,

  // Modules for dev and build (recommended): https://go.nuxtjs.dev/config-modules
  buildModules: [
  ],

  // Modules: https://go.nuxtjs.dev/config-modules
  modules: [
  ],

  //maple
  publicRuntimeConfig: {
    conf: fs.readFileSync('/Users/maple/.cam.conf', 'utf8')
  },

  serverMiddleware: [
    { path: "/get_config", handler: "~/server-middleware/get_config.js" },
    { path: "/redis", handler: "~/server-middleware/redis.js" }
  ],

  // Build Configuration: https://go.nuxtjs.dev/config-build
  build: {
    transpile: [/^element-ui/],
  },

  server: {
    port: 8257 // default: 3000
  }

}
