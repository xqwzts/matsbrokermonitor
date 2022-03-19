// For "synthetic links" to not do anything
function matsbm_noclick(event) {
    event.preventDefault();
    return false;
}


// ::: BROWSE QUEUE

function matsbm_reissue_bulk(event, queueId) {
    // ?: Is it disabled?
    if (document.getElementById("matsbm_reissue_bulk").classList.contains('matsbm_button_disabled')) {
        // -> Yes, disabled - so ignore press.
        return;
    }
    matsbm_reissue_or_delete_bulk(event, queueId, "reissue")
}

function matsbm_delete_propose_bulk(event) {
    // ?: Is it disabled?
    if (document.getElementById("matsbm_delete_bulk").classList.contains('matsbm_button_disabled')) {
        // -> Yes, disabled - so ignore press.
        return;
    }
    // Gray out Delete
    document.getElementById("matsbm_delete_bulk").classList.add("matsbm_button_disabled");
    // Set Delete Confirm and Delete Cancel visible
    document.getElementById("matsbm_delete_confirm_bulk").classList.remove("matsbm_button_hidden");
    document.getElementById("matsbm_delete_cancel_bulk").classList.remove("matsbm_button_hidden");
}

function matsbm_delete_cancel_bulk(event) {
    // Set Delete Confirm and Delete Cancel hidden
    document.getElementById("matsbm_delete_confirm_bulk").classList.add("matsbm_button_hidden");
    document.getElementById("matsbm_delete_cancel_bulk").classList.add("matsbm_button_hidden");
    // Enable Delete
    document.getElementById("matsbm_delete_bulk").classList.remove("matsbm_button_disabled");
}

// Action for the "Confirm Delete" button made visible by clicking "Delete".
function matsbm_delete_confirmed_bulk(event, queueId) {
    matsbm_reissue_or_delete_bulk(event, queueId, "delete")
}

function matsbm_checkall(event) {
    for (const checkbox of document.body.querySelectorAll(".matsbm_checkmsg")) {
        checkbox.checked = event.target.checked;
    }
    matsbm_evaluate_checkall_and_buttons();
}

function matsbm_checkmsg(event) {
    matsbm_evaluate_checkall_and_buttons();
}

function matsbm_checkinvert(event) {
    for (const checkbox of document.body.querySelectorAll(".matsbm_checkmsg")) {
        checkbox.checked = !checkbox.checked;
    }
    matsbm_evaluate_checkall_and_buttons();
}

function matsbm_evaluate_checkall_and_buttons() {
    // :: Handle "check all" based on whether checkboxes are checked.
    let numchecked = 0;
    let numunchecked = 0;
    let allchecked = true;
    let allunchecked = true;
    for (const checkbox of document.body.querySelectorAll(".matsbm_checkmsg")) {
        if (checkbox.checked) {
            // -> checked
            numchecked++;
            allunchecked = false;
        } else {
            // -> unchecked
            numunchecked++;
            allchecked = false;
        }
    }
    const checkall = document.getElementById('matsbm_checkall');

    // ?? Handle all checked, none checked, or anything between
    if (numchecked && numunchecked) {
        // -> Some of both
        checkall.indeterminate = true;
    } else if (allchecked) {
        // -> All checked
        checkall.indeterminate = false;
        checkall.checked = true;
    } else if (allunchecked) {
        // -> All unchecked
        checkall.indeterminate = false;
        checkall.checked = false;
    }

    // We're changing selection: Cancel the "Confirm Delete" if it was active.
    matsbm_delete_cancel_bulk();

    // Activate or deactivate Reissue/Delete based on whether any is selected.
    const reissueBtn = document.getElementById('matsbm_reissue_bulk');
    const deleteBtn = document.getElementById('matsbm_delete_bulk');
    if (numchecked) {
        reissueBtn.classList.remove('matsbm_button_disabled')
        deleteBtn.classList.remove('matsbm_button_disabled')
    } else {
        reissueBtn.classList.add('matsbm_button_disabled')
        deleteBtn.classList.add('matsbm_button_disabled')
    }

    // Update selection text
    let selectionText = "Messages in list: " + (numchecked + numunchecked);
    if (allunchecked) {
        selectionText += ", no selected messages";
    } else if (allchecked) {
        selectionText += ", ALL messages selected";
    } else {
        selectionText += ". Selected: " + numchecked + ", not selected:" + numunchecked;
    }


    document.getElementById("matsbm_num_messages_shown").textContent = selectionText;

}

function matsbm_reissue_or_delete_bulk(event, queueId, action) {
    // hide Cancel Delete and Confirm Delete
    document.getElementById("matsbm_delete_confirm_bulk").classList.add("matsbm_button_hidden");
    document.getElementById("matsbm_delete_cancel_bulk").classList.add("matsbm_button_hidden");
    // Disable Reissue and Delete buttons
    document.getElementById('matsbm_reissue_bulk').classList.add('matsbm_button_disabled');
    document.getElementById('matsbm_delete_bulk').classList.add('matsbm_button_disabled');

    // :: Find which messages
    const msgSysMsgIds = [];
    for (const checkbox of document.body.querySelectorAll(".matsbm_checkmsg")) {
        if (checkbox.checked) {
            msgSysMsgIds.push(checkbox.getAttribute("data-msgid"));
        }
    }
    const actionPresent = action === "reissue" ? "Reissuing" : "Deleting";
    const actionPast = action === "reissue" ? "reissued" : "deleted";
    const operation = action === "reissue" ? "PUT" : "DELETE";

    document.getElementById('matsbm_action_message').textContent
        = actionPresent + " " + msgSysMsgIds.length + " message" + (msgSysMsgIds.length > 1 ? "s" : "") + ".";

    let jsonPath = window.matsbm_json_path ? window.matsbm_json_path : window.location.pathname;
    let requestBody = {
        action: action,
        queueId: queueId,
        msgSysMsgIds: msgSysMsgIds
    };
    fetch(jsonPath, {
        method: operation,
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    })
        .then(response => {
            if (!response.ok) {
                console.error("Response not OK", response);
                document.getElementById('matsbm_action_message').textContent
                    = "Error! HTTP Status: " + response.status;
                return;
            }
            response.json().then(result => {
                document.getElementById('matsbm_action_message').textContent
                    = "Done, " + result.numberOfAffectedMessages
                    + " message" + (result.numberOfAffectedMessages > 1 ? "s" : "") + " " + actionPast
                    + (action === 'reissue' ? " (Check console for new message ids)." : ".");
                for (const msgSysMsgId of result.msgSysMsgIds) {
                    const row = document.getElementById('matsbm_msgid_' + msgSysMsgId);
                    if (row) {
                        row.classList.add('matsbm_' + actionPast);
                    } else {
                        console.error("Couldn't find message row for msgSysMsgId [" + msgSysMsgId + "].");
                    }
                }
                if (action === "reissue") {
                    console.log("Reissued MsgSysMsgIds:", result.reissuedMsgSysMsgIds);
                }
                setTimeout(() => window.location.reload(), 3500);
            });
        })
        .catch(error => {
            console.error("Fetch error", error);
            document.getElementById('matsbm_action_message').textContent
                = "Error! " + error;
        });
}


// ::: EXAMINE MESSAGE

// :: REISSUE / DELETE

function matsbm_reissue_single(event, queueId, msgSysMsgId) {
    matsbm_reissue_or_delete_single(event, queueId, msgSysMsgId, "reissue")
}

function matsbm_delete_propose_single(event) {
    // Gray out Delete
    document.getElementById("matsbm_delete_single").classList.add("matsbm_button_disabled");
    // Set Delete Confirm and Delete Cancel visible
    document.getElementById("matsbm_delete_confirm_single").classList.remove("matsbm_button_hidden");
    document.getElementById("matsbm_delete_cancel_single").classList.remove("matsbm_button_hidden");
}

function matsbm_delete_cancel_single(event) {
    // Set Delete Confirm and Delete Cancel hidden
    document.getElementById("matsbm_delete_confirm_single").classList.add("matsbm_button_hidden");
    document.getElementById("matsbm_delete_cancel_single").classList.add("matsbm_button_hidden");
    // Enable Delete
    document.getElementById("matsbm_delete_single").classList.remove("matsbm_button_disabled");
}

// Action for the "Confirm Delete" button made visible by clicking "Delete".
function matsbm_delete_confirmed_single(event, queueId, msgSysMsgId) {
    matsbm_reissue_or_delete_single(event, queueId, msgSysMsgId, "delete")
}


function matsbm_reissue_or_delete_single(event, queueId, msgSysMsgId, action) {
    // hide Cancel Delete and Confirm Delete
    document.getElementById("matsbm_delete_confirm_single").classList.add("matsbm_button_hidden");
    document.getElementById("matsbm_delete_cancel_single").classList.add("matsbm_button_hidden");
    // Disable Reissue and Delete buttons
    document.getElementById('matsbm_reissue_single').classList.add('matsbm_button_disabled');
    document.getElementById('matsbm_delete_single').classList.add('matsbm_button_disabled');

    const actionPresent = action === "reissue" ? "Reissuing" : "Deleting";
    const actionPast = action === "reissue" ? "reissued" : "deleted";
    const operation = action === "reissue" ? "PUT" : "DELETE";

    document.getElementById('matsbm_action_message').textContent
        = actionPresent + " message [" + msgSysMsgId + "].";

    let jsonPath = window.matsbm_json_path ? window.matsbm_json_path : window.location.pathname;
    let requestBody = {
        action: action,
        queueId: queueId,
        msgSysMsgIds: [msgSysMsgId]
    };
    fetch(jsonPath, {
        method: operation,
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    })
        .then(response => {
            if (!response.ok) {
                console.error("Response not OK", response);
                document.getElementById('matsbm_action_message').textContent
                    = "Error! HTTP Status: " + response.status;
                return;
            }
            response.json().then(result => {
                let actionMessage;
                if (result.numberOfAffectedMessages !== 1) {
                    actionMessage = "Message wasn't " + actionPast + "! Already " + actionPast + "?"
                } else {
                    const msgSysMsgId = result.msgSysMsgIds[0];
                    actionMessage = "Message " + actionPast + "!"
                }
                document.getElementById('matsbm_action_message').textContent
                    = actionMessage
                    + (action === 'reissue' ? " (Check console for new message id)." : "");
                if (action === "reissue") {
                    console.log("Reissued MsgSysMsgIds:", result.reissuedMsgSysMsgIds);
                }
                setTimeout(() => {
                    document.getElementById('matsbm_part_flow_and_message_props').classList.add('matsbm_part_hidden');
                    document.getElementById('matsbm_part_state_and_message').classList.add('matsbm_part_hidden');
                    document.getElementById('matsbm_part_matstrace').classList.add('matsbm_part_hidden');
                    setTimeout(() => window.location = window.location.pathname
                        + "?browse&destinationId=queue:" + queueId, 2000);
                }, 750);
            });
        })
        .catch(error => {
            console.error("Fetch error", error);
            document.getElementById('matsbm_action_message').textContent
                = "Error! " + error;
        });
}


// :: MATSTRACE CALL MODAL

let matsbm_activecallmodal = -1;

function matsbm_clearcallmodal(event) {
    // Clear the "underlay" for the modal
    console.log("Clear modal");
    console.log(event)
    console.log(event.target)
    let modalunderlay = document.getElementById("matsbm_callmodalunderlay");
    // Don't clear if the target is the modal, to enable interaction with it.
    if (event.target !== modalunderlay) {
        return;
    }
    console.log(modalunderlay);

    matsbm_clearcallmodal_noncond()

    return true;
}

function matsbm_clearcallmodal_noncond() {
    // Clear the modal underlay
    document.getElementById("matsbm_callmodalunderlay").classList.remove("matsbm_callmodalunderlay_visible")

    // Clear all call modals
    for (const modal of document.getElementsByClassName("matsbm_box_call_and_state_modal")) {
        modal.classList.remove("matsbm_box_call_and_state_modal_visible");
    }
    // Clear all call rows
    for (const row of document.body.querySelectorAll("#matsbm_table_matstrace tr")) {
        row.classList.remove("matsbm_row_active");
    }

    matsbm_activecallmodal = -1;
}

function matsbm_callmodal(event) {
    // Un-hide on the specific call modal
    let tr = event.target.closest("tr");
    let callno = tr.getAttribute("data-callno");
    let callmodal = document.getElementById("matsbm_callmodal_" + callno);
    console.log(callmodal);
    callmodal.classList.add("matsbm_box_call_and_state_modal_visible");

    // Un-hide the "underlay"
    let modalunderlay = document.getElementById("matsbm_callmodalunderlay");
    modalunderlay.classList.add("matsbm_callmodalunderlay_visible")

    // Make Call row active
    let callRow = document.getElementById("matsbm_callrow_" + callno);
    let processRow = document.getElementById("matsbm_processrow_" + callno);
    callRow.classList.add("matsbm_row_active")
    processRow.classList.add("matsbm_row_active")

    // Clear hover
    matsbm_hover_call_out();

    // Set the active call number
    matsbm_activecallmodal = callno;
}

// Modal Key listener: when modal is active.
document.addEventListener('keydown', (event) => {
    if (matsbm_activecallmodal < 0) {
        return;
    }
    var name = event.key;
    var code = event.code;
    console.log(`Key pressed ${name} \r\n Key code value: ${code}`);

    if (name === "Escape") {
        matsbm_clearcallmodal_noncond();
    }

    let currentCallModal = document.getElementById("matsbm_callmodal_" + matsbm_activecallmodal);
    let currentCallRow = document.getElementById("matsbm_callrow_" + matsbm_activecallmodal);
    let currentProcessRow = document.getElementById("matsbm_processrow_" + matsbm_activecallmodal);
    if (name === "ArrowUp") {
        matsbm_activecallmodal--;
        let nextCallModal = document.getElementById("matsbm_callmodal_" + matsbm_activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsbm_row_active")
            currentProcessRow.classList.remove("matsbm_row_active")
            let nextCallRow = document.getElementById("matsbm_callrow_" + matsbm_activecallmodal);
            let nextProcessRow = document.getElementById("matsbm_processrow_" + matsbm_activecallmodal);
            nextCallRow.classList.add("matsbm_row_active")
            nextProcessRow.classList.add("matsbm_row_active")
            // Handle the sticky header, so scroll above it
            if (matsbm_activecallmodal === 1) {
                let top = document.body.querySelector("#matsbm_table_matstrace thead");
                top.scrollIntoView({behavior: "smooth", block: "nearest"});
            } else {
                document.getElementById("matsbm_callrow_" + (matsbm_activecallmodal - 1))
                    .scrollIntoView({behavior: "smooth", block: "nearest"});
            }
        } else {
            // Back out
            matsbm_activecallmodal++;
        }
        event.preventDefault();
    }
    if (name === "ArrowDown") {
        matsbm_activecallmodal++;
        let nextCallModal = document.getElementById("matsbm_callmodal_" + matsbm_activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsbm_row_active")
            currentProcessRow.classList.remove("matsbm_row_active")
            let nextCallRow = document.getElementById("matsbm_callrow_" + matsbm_activecallmodal);
            let nextProcessRow = document.getElementById("matsbm_processrow_" + matsbm_activecallmodal);
            nextCallRow.classList.add("matsbm_row_active")
            nextProcessRow.classList.add("matsbm_row_active")
            nextCallRow.scrollIntoView({behavior: "smooth", block: "nearest"});
        } else {
            // Back out
            matsbm_activecallmodal--;
        }
        event.preventDefault();
    }
}, false);


// :: MATSTRACE HOVER

function matsbm_hover_call(event) {
    let tr = event.target.closest("tr");
    let callno = tr.getAttribute("data-callno");
    let callRow = document.getElementById("matsbm_callrow_" + callno);
    let processRow = document.getElementById("matsbm_processrow_" + callno);
    callRow.classList.add("matsbm_row_hover")
    processRow.classList.add("matsbm_row_hover")
}

function matsbm_hover_call_out() {
    for (const row of document.body.querySelectorAll("#matsbm_table_matstrace tr")) {
        row.classList.remove("matsbm_row_hover");
    }
}