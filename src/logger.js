function timestamp() {
    return new Date().toISOString();
}

function info(message) {
    console.log(`[${timestamp()}] [INFO] ${message}`);
}

function success(message) {
    console.log(`[${timestamp()}] [OK] ${message}`);
}

function warn(message) {
    console.warn(`[${timestamp()}] [WARN] ${message}`);
}

function error(message, err) {
    console.error(`[${timestamp()}] [ERROR] ${message}`);
    if (err) {
        console.error(err.stack || err.message || err);
    }
}

module.exports = { info, success, warn, error };
