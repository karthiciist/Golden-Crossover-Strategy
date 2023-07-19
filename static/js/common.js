function setLoader(container, loader) {
    if(loader) {
        $(container).find('.loader').addClass('d-flex').removeClass('d-none');
        $(container).find('.content').addClass('d-none').removeClass('d-flex');
    } else {
        $(container).find('.loader').addClass('d-none').removeClass('d-flex');
        $(container).find('.content').removeClass('d-none').addClass('d-flex');
    }
}

function set_button_loader(selector, flag) {
    $(selector).prop('disabled', flag)
    if(flag) {
        $(selector).find('.loader').removeClass('d-none')
        $(selector).find('.loader').siblings().addClass('d-none')
    } else {
        $(selector).find('.loader').addClass('d-none')
        $(selector).find('.loader').siblings().removeClass('d-none')
    }
}

async function normalize_response(response) {
    let result = await response
    if(result.hasOwnProperty('Status')) {
        result.status = result.Status
        delete result.Status
    }
    if(result.hasOwnProperty('Message')) {
        result.message = result.Message
        delete result.Message
    }
    return result
}

async function handleResponse(response) {
    let result = {}
    let isJSONResponse = response.headers.get('Content-Type') == 'application/json'
    switch(response.status) {
        case 404:
            result = isJSONResponse ? normalize_response(response.json()) : {status : 'Failure', message : '404 (Not Found)'}
            break;
        case 401:
            window.location.href = baseURL + '/'
            break;
        case 200:
            result = isJSONResponse ? normalize_response(response.json()): {status : 'Failure', message : 'API response format other than JSON not supported.'}
            break;
        default:
            result = isJSONResponse ? normalize_response(response.json()): {status : 'Failure', message : `API Response code (${response.status}) not supported.`}
            break;
    }
    return result;
}

async function request(url, options = {}) {
    let response
    try {
        response = await fetch(base_url + url, options)
    } catch(e) {
        return { status : "Failure", message : e.message}
    }
    return handleResponse(response);
}

function flash_message(selector, message) {
    $(selector).text(message)
    setTimeout(() => {
        $(selector).text('')
    }, 3000)
}