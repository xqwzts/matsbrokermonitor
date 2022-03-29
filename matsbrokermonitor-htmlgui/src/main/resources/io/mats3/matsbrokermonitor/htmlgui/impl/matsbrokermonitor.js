// For "synthetic links" to not do anything
function matsbm_noclick(event) {
    event.preventDefault();
    return false;
}

// Global key listener, dispatching to relevant "sub-listener"
document.addEventListener('keydown', (event) => {
    // ?: We only care about the "pure" letters, not with any modifiers (e.g. Ctrl+R shall be reload, not reissue!)
    if (event.ctrlKey || event.altKey || event.metaKey) {
        // -> Modifiers present, ignore
        return;
    }

    if (document.getElementById("matsbm_page_browse_queue")) {
        matsbm_browseQueueKeyListener(event);
    } else if (document.getElementById("matsbm_page_examine_message")) {
        if (matsbm_is_call_modal_active()) {
            matsbm_modalActiveKeyListener(event);
        } else {
            matsbm_examineMessageKeyListener(event);
        }
    }
}, false);


// ::: BROKER OVERVIEW

function matsbm_view_all_destinations(event) {
    console.log("view all")
    // :: ToC EndpointGroup
    for (const tocEpGroupRow of document.body.querySelectorAll(".matsbm_toc_endpointgroup")) {
        // -> Remove the "hidden" class
        tocEpGroupRow.classList.remove("matsbm_marker_hidden_toc");
    }

    // :: Endpoints (rows in table)
    for (const epRow of document.body.querySelectorAll(".matsbm_endpoint_group_row")) {
        // -> Remove the "hidden" class
        epRow.classList.remove("matsbm_marker_hidden_row");
    }

    // :: EndpointGroups (divs with endpoint-table inside)
    for (const epGroupDiv of document.body.querySelectorAll(".matsbm_endpoint_group")) {
        // -> Remove the "hidden" class
        epGroupDiv.classList.remove("matsbm_marker_hidden_epgrp");
    }

    document.getElementById("matsbm_button_viewall").classList.add('matsbm_button_active')
    document.getElementById("matsbm_button_viewbad").classList.remove('matsbm_button_active')
}

function matsbm_view_bad_destinations(event) {
    console.log("view bad")
    // :: ToC EndpointGroup
    for (const tocEpGroupRow of document.body.querySelectorAll(".matsbm_toc_endpointgroup")) {
        // ?: Does the ToC entry NOT have any of the "bad" markers?
        if (!(tocEpGroupRow.classList.contains("matsbm_marker_has_old_msgs")
            || tocEpGroupRow.classList.contains("matsbm_marker_has_dlqs"))) {
            // -> No, does not have "bad" markers: Hide them
            tocEpGroupRow.classList.add("matsbm_marker_hidden_toc");
        }
    }

    // :: Endpoints (rows in table)
    for (const epRow of document.body.querySelectorAll(".matsbm_endpoint_group_row")) {
        // ?: Does the group NOT have any of the "bad" markers?
        if (!(epRow.classList.contains("matsbm_marker_has_old_msgs")
            || epRow.classList.contains("matsbm_marker_has_dlqs"))) {
            // -> No, does not have "bad" markers: Hide them
            epRow.classList.add("matsbm_marker_hidden_row");
        }
    }

    // :: EndpointGroups (divs with endpoint-table inside)
    for (const epGroupDiv of document.body.querySelectorAll(".matsbm_endpoint_group")) {
        // ?: Does the group NOT have any of the "bad" markers?
        if (!(epGroupDiv.classList.contains("matsbm_marker_has_old_msgs")
            || epGroupDiv.classList.contains("matsbm_marker_has_dlqs"))) {
            // -> No, does not have "bad" markers: Hide them
            epGroupDiv.classList.add("matsbm_marker_hidden_epgrp");
        }
    }

    document.getElementById("matsbm_button_viewall").classList.remove('matsbm_button_active')
    document.getElementById("matsbm_button_viewbad").classList.add('matsbm_button_active')
}


// ::: BROWSE QUEUE

// Key Listener for Browse Queue
function matsbm_browseQueueKeyListener(event) {
    const name = event.key;
    if (name === "Escape") {
        // ?: Is the "Confirm Delete" button active (non-hidden)?
        if (matsbm_is_delete_confirm_bulk_active()) {
            // -> Yes it is active - then it is this we'll escape
            matsbm_delete_cancel_bulk();
        } else {
            setTimeout(() => {
                document.getElementById("matsbm_back_broker_overview").click();
            }, 10);
        }
        return;
    }

    if (name.toLowerCase() === "r") {
        document.getElementById("matsbm_reissue_bulk").click();
    }
    if (name.toLowerCase() === "d") {
        document.getElementById("matsbm_delete_bulk").click();
    }
    if ((name.toLowerCase() === "x") && matsbm_is_delete_confirm_bulk_active()) {
        document.getElementById("matsbm_delete_confirm_bulk").click();
    }

}

function matsbm_reissue_bulk(event, queueId) {
    // ?: Is it disabled?
    if (document.getElementById("matsbm_reissue_bulk").classList.contains('matsbm_button_disabled')) {
        // -> Yes, disabled - so ignore press.
        return;
    }
    matsbm_reissue_or_delete_bulk(event, queueId, "reissue")
}

function matsbm_is_delete_confirm_bulk_active() {
    // Return whether the "Confirm Delete" button is non-hidden
    const confirmDeleteButton = document.getElementById("matsbm_delete_confirm_bulk");
    if (!confirmDeleteButton) {
        return false;
    }
    return !confirmDeleteButton.classList.contains('matsbm_button_hidden');

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

    document.getElementById('matsbm_action_message').textContent = actionPresent + " " + msgSysMsgIds.length + " message" + (msgSysMsgIds.length > 1 ? "s" : "") + ".";

    let jsonPath = window.matsbm_json_path ? window.matsbm_json_path : window.location.pathname;
    let requestBody = {
        action: action, queueId: queueId, msgSysMsgIds: msgSysMsgIds
    };
    fetch(jsonPath, {
        method: operation, headers: {
            'Content-Type': 'application/json'
        }, body: JSON.stringify(requestBody)
    })
        .then(response => {
            if (!response.ok) {
                console.error("Response not OK", response);
                document.getElementById('matsbm_action_message').textContent = "Error! HTTP Status: " + response.status;
                return;
            }
            response.json().then(result => {
                document.getElementById('matsbm_action_message').textContent = "Done, " + result.numberOfAffectedMessages + " message" + (result.numberOfAffectedMessages > 1 ? "s" : "") + " " + actionPast + (action === 'reissue' ? " (Check console for new message ids)." : ".");
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
                // Annoying CSS "delayed transition" also somehow "overwrites" the row color transition..?!
                // Using JS hack instead, to delay second part of transition.
                setTimeout(() => {
                    for (const msgSysMsgId of result.msgSysMsgIds) {
                        const row = document.getElementById('matsbm_msgid_' + msgSysMsgId);
                        if (row) {
                            row.classList.add('matsbs_delete_or_reissue');
                        }
                    }
                    setTimeout(() => window.location.reload(), 1000);
                }, 1500)
            });
        })
        .catch(error => {
            console.error("Fetch error", error);
            document.getElementById('matsbm_action_message').textContent = "Error! " + error;
        });
}

// ::: EXAMINE MESSAGE

// .. Key Listener for Examine Message

function matsbm_examineMessageKeyListener(event) {
    const name = event.key;
    if (name === "Escape") {
        // ?: Is the "Confirm Delete" button active (non-hidden)?
        if (matsbm_is_delete_confirm_single_active()) {
            // -> Yes it is active - then it is this we'll escape
            matsbm_delete_cancel_single();
        } else {
            setTimeout(() => {
                document.getElementById("matsbm_back_browse_queue").click();
            }, 10);
        }
        return;
    }


    if (name.toLowerCase() === "r") {
        document.getElementById("matsbm_reissue_single").click();
    }
    if (name.toLowerCase() === "d") {
        document.getElementById("matsbm_delete_single").click();
    }
    if ((name.toLowerCase() === "x") && matsbm_is_delete_confirm_single_active()) {
        document.getElementById("matsbm_delete_confirm_single").click();
    }
}

// .. REISSUE / DELETE

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

function matsbm_is_delete_confirm_single_active() {
    // Return whether the "Confirm Delete" button is non-hidden
    const confirmDeleteButton = document.getElementById("matsbm_delete_confirm_single");
    if (!confirmDeleteButton) {
        return false;
    }
    return !confirmDeleteButton.classList.contains('matsbm_button_hidden');
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

    document.getElementById('matsbm_action_message').textContent = actionPresent + " message [" + msgSysMsgId + "].";

    let jsonPath = window.matsbm_json_path ? window.matsbm_json_path : window.location.pathname;
    let requestBody = {
        action: action, queueId: queueId, msgSysMsgIds: [msgSysMsgId]
    };
    fetch(jsonPath, {
        method: operation, headers: {
            'Content-Type': 'application/json'
        }, body: JSON.stringify(requestBody)
    })
        .then(response => {
            if (!response.ok) {
                console.error("Response not OK", response);
                document.getElementById('matsbm_action_message').textContent = "Error! HTTP Status: " + response.status;
                return;
            }
            response.json().then(result => {
                let actionMessage;
                if (result.numberOfAffectedMessages !== 1) {
                    actionMessage = "Message wasn't " + actionPast + "! Already " + actionPast + "?"
                } else {
                    actionMessage = "Message " + actionPast + "!"
                }
                document.getElementById('matsbm_action_message').textContent = actionMessage + (action === 'reissue' ? " (Check console for new message id)." : "");
                if (action === "reissue") {
                    console.log("Reissued MsgSysMsgIds:", result.reissuedMsgSysMsgIds);
                }
                setTimeout(() => {
                    document.getElementById('matsbm_part_flow_and_message_props').classList.add('matsbm_part_hidden');
                    document.getElementById('matsbm_part_state_and_message').classList.add('matsbm_part_hidden');
                    document.getElementById('matsbm_part_matstrace').classList.add('matsbm_part_hidden');
                    setTimeout(() => window.location = window.location.pathname + "?browse&destinationId=queue:" + queueId,
                        2000);
                }, 750);
            });
        })
        .catch(error => {
            console.error("Fetch error", error);
            document.getElementById('matsbm_action_message').textContent = "Error! " + error;
        });
}


// .. MATSTRACE CALL MODAL

let matsbm_activecallmodal = -1;

function matsbm_is_call_modal_active() {
    // Return whether the active call modal is not -1.
    return matsbm_activecallmodal !== -1;
}

function matsbm_clearcallmodal(event) {
    // Clear the "underlay" for the modal
    let modalunderlay = document.getElementById("matsbm_callmodalunderlay");
    // Don't clear if the target is the modal, to enable interaction with it.
    if (event.target !== modalunderlay) {
        return;
    }

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
    // Clear the "Confirm Delete" if active
    matsbm_delete_cancel_single();

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
function matsbm_modalActiveKeyListener(event) {
    const name = event.key;
    const code = event.code;
    console.log(`Key pressed ${name} \r\n Key code value: ${code}`);

    // ?: Was the Escape?
    if (name === "Escape") {
        // -> Yes, and call modal is active - cancel it.
        matsbm_clearcallmodal_noncond();
        return;
    }

    const currentCallModal = document.getElementById("matsbm_callmodal_" + matsbm_activecallmodal);
    const currentCallRow = document.getElementById("matsbm_callrow_" + matsbm_activecallmodal);
    const currentProcessRow = document.getElementById("matsbm_processrow_" + matsbm_activecallmodal);
    if (name === "ArrowUp") {
        matsbm_activecallmodal--;
        const nextCallModal = document.getElementById("matsbm_callmodal_" + matsbm_activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsbm_row_active")
            currentProcessRow.classList.remove("matsbm_row_active")
            const nextCallRow = document.getElementById("matsbm_callrow_" + matsbm_activecallmodal);
            const nextProcessRow = document.getElementById("matsbm_processrow_" + matsbm_activecallmodal);
            nextCallRow.classList.add("matsbm_row_active")
            nextProcessRow.classList.add("matsbm_row_active")
            // Handle the sticky header, so scroll above it
            if (matsbm_activecallmodal === 1) {
                const top = document.body.querySelector("#matsbm_table_matstrace thead");
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
        const nextCallModal = document.getElementById("matsbm_callmodal_" + matsbm_activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsbm_row_active")
            currentProcessRow.classList.remove("matsbm_row_active")
            const nextCallRow = document.getElementById("matsbm_callrow_" + matsbm_activecallmodal);
            const nextProcessRow = document.getElementById("matsbm_processrow_" + matsbm_activecallmodal);
            nextCallRow.classList.add("matsbm_row_active")
            nextProcessRow.classList.add("matsbm_row_active")
            nextCallRow.scrollIntoView({behavior: "smooth", block: "nearest"});
        } else {
            // Back out
            matsbm_activecallmodal--;
        }
        event.preventDefault();
    }
}


// .. MATSTRACE HOVER

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