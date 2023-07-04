chrome.runtime.onMessage.addListener( (request, sender, sendResponse) => {
    if (request.contentScriptQuery === 'check_sarcasm') {
        fetch(request.url, {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            body: JSON.stringify({'textComments': request.data})
        })
            .then(response => response.json())
            .then(response => sendResponse(response))
            .catch(error => console.log('Error:', error))
    }
})
