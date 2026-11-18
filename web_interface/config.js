(function () {
    const DEFAULT_API_BASE_URL = 'http://192.168.0.50:5000';

    window.APP_CONFIG = Object.assign({
        API_BASE_URL: DEFAULT_API_BASE_URL
    }, window.APP_CONFIG || {});

    if (!window.APP_CONFIG.buildApiUrl) {
        window.APP_CONFIG.buildApiUrl = function buildApiUrl(path) {
            if (typeof path !== 'string') {
                throw new TypeError('O caminho da API deve ser uma string.');
            }

            const normalizedPath = path.startsWith('/') ? path : `/${path}`;
            return `${window.APP_CONFIG.API_BASE_URL}${normalizedPath}`;
        };
    }
})();
