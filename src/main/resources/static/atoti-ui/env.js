const baseUrl = window.location.href.split('/ui')[0];
const version = "6.1.3";

window.env = {
  contentServer: {
    url: baseUrl,
    version,
  },
  atotiServers: {
    "MAC": {
      url: baseUrl,
      version,
    },
  },
  jwtServer: {
    url: baseUrl,
    version,
  }
};
