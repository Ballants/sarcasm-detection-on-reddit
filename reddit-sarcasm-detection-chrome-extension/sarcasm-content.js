let comments_predictions_queue = {}
let comments_predictions = {}
const link = 'http://localhost:5000/prediction'

const waitForTargetElement = (elementID) => {
    return new Promise((resolve) => {
        if (document.getElementById(elementID)) {
            return resolve(document.getElementById(elementID))
        }
        const observer = new MutationObserver(() => {
            if (document.getElementById(elementID)) {
                resolve(document.getElementById(elementID))
                observer.disconnect()
            }
        })
        observer.observe(document.body, config)
    })
}

let last_insert = Date.now()

const fetch_prediction = (commentID, commentText) => {
    if (!(commentID in comments_predictions_queue)) {
        comments_predictions_queue[commentID] = commentText
        last_insert = Date.now()

        // console.count('Aggiunto commento alla coda')

    }
}

let isFetchInPending = false

const getComments = (mutations) => {

    for (let mutation of mutations) {
        for (let node of mutation.addedNodes) {

            if (node.querySelector) {

                const ids = Array.from(document.getElementsByClassName('Comment')).map(node => node.classList[1])

                if (!!ids.length) {

                    ids.forEach(commentID => {

                            if (!(commentID in comments_predictions)) {
                                const comment = document.getElementById(commentID)
                                if (comment) {
                                    const commentTextElement = comment.querySelector('[data-testid="comment"]')

                                    if (commentTextElement?.innerText) {

                                        const commentText = commentTextElement?.innerText

                                        fetch_prediction(commentID, commentText)
                                    }
                                }
                            }


                        }
                    )
                }
            }
        }
    }

    /**
     * fetch prediction request after 3 second from the last insert comment
     * */
    if (Object.keys(comments_predictions_queue).length > 0 && last_insert + 3000 < Date.now() && !isFetchInPending) {

        const request = Object.keys(comments_predictions_queue).map(id => ({
            id,
            comment: comments_predictions_queue[id]
        }))
        isFetchInPending = true

        // console.log('fetch request')

        chrome.runtime.sendMessage(
            {
                contentScriptQuery: 'check_sarcasm',
                data: request,
                url: link
            },
            (predictions) => {

                predictions?.map(({id, prediction}) => {
                    comments_predictions[id] = prediction
                    if (prediction === 1.0) {
                        const comment = document.getElementById(id)
                        let nodeComment = comment.querySelector('[data-testid="comment"]')

                        nodeComment.style.boxShadow = 'inset #FFD700 0px 0px 10px 3px'
                        nodeComment.style.padding = '10px'
                        nodeComment.style.paddingTop = '14px'

                        let sarcasmElement = document.createElement('div')
                        sarcasmElement.style.display = 'flex'
                        sarcasmElement.style.justifyContent = 'center'
                        sarcasmElement.style.padding = '6px'
                        sarcasmElement.textContent = '---- It\'s just SARCASM ----'

                        nodeComment.insertBefore(sarcasmElement, nodeComment.firstChild)
                    } else {
                        const comment = document.getElementById(id)
                        let nodeComment = comment.querySelector('[data-testid="comment"]')

                        nodeComment.style.boxShadow = 'inset #acaeb0 0px 0px 10px 3px'
                        nodeComment.style.padding = '10px'
                        nodeComment.style.paddingTop = '14px'

                        let noSarcasmElement = document.createElement('div')
                        noSarcasmElement.style.display = 'flex'
                        noSarcasmElement.style.justifyContent = 'center'
                        noSarcasmElement.style.padding = '6px'
                        noSarcasmElement.textContent = '---- It\'s NOT sarcasm ----'

                        nodeComment.insertBefore(noSarcasmElement, nodeComment.firstChild)
                    }
                })
                comments_predictions_queue = {}
                isFetchInPending = false

            }
        )
    }
}

MutationObserver = window.MutationObserver

let observer = new MutationObserver(getComments)
const config = {subtree: true, childList: true}

const observerOnTargetCallback = async () => {
    const target = await waitForTargetElement('overlayScrollContainer')
    observer.observe(target, config)
}

window.onload = observerOnTargetCallback
