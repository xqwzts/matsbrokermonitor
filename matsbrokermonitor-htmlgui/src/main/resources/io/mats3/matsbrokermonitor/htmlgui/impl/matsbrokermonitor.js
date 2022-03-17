// For "synthetic links" to not do anything
function matsbm_noclick(event) {
    event.preventDefault();
    return false;
}


// ::: BROWSE QUEUE

function matsmb_reissue_bulk(event, queueId) {
    // ?: Is it disabled?
    if (document.getElementById("matsmb_reissue_bulk").classList.contains('matsmb_button_disabled')) {
        // -> Yes, disabled - so ignore press.
        return;
    }
    matsmb_reissue_or_delete_bulk(event, queueId, "reissue")
}

function matsmb_delete_propose_bulk(event) {
    // ?: Is it disabled?
    if (document.getElementById("matsmb_delete_bulk").classList.contains('matsmb_button_disabled')) {
        // -> Yes, disabled - so ignore press.
        return;
    }
    // Gray out Delete
    document.getElementById("matsmb_delete_bulk").classList.add("matsmb_button_disabled");
    // Set Delete Confirm and Delete Cancel visible
    document.getElementById("matsmb_delete_confirm_bulk").classList.remove("matsmb_button_hidden");
    document.getElementById("matsmb_delete_cancel_bulk").classList.remove("matsmb_button_hidden");
}

function matsmb_delete_cancel_bulk(event) {
    // Set Delete Confirm and Delete Cancel hidden
    document.getElementById("matsmb_delete_confirm_bulk").classList.add("matsmb_button_hidden");
    document.getElementById("matsmb_delete_cancel_bulk").classList.add("matsmb_button_hidden");
    // Enable Delete
    document.getElementById("matsmb_delete_bulk").classList.remove("matsmb_button_disabled");
}

// Action for the "Confirm Delete" button made visible by clicking "Delete".
function matsmb_delete_confirmed_bulk(event, queueId) {
    matsmb_reissue_or_delete_bulk(event, queueId, "delete")
}

function matsmb_checkall(event) {
    for (const checkbox of document.body.querySelectorAll(".matsmb_checkmsg")) {
        checkbox.checked = event.target.checked;
    }
    matsmb_evaluate_checkall_and_buttons();
}

function matsmb_checkmsg(event) {
    matsmb_evaluate_checkall_and_buttons();
}

function matsmb_checkinvert(event) {
    for (const checkbox of document.body.querySelectorAll(".matsmb_checkmsg")) {
        checkbox.checked = !checkbox.checked;
    }
    matsmb_evaluate_checkall_and_buttons();
}

function matsmb_evaluate_checkall_and_buttons() {
    // :: Handle "check all" based on whether checkboxes are checked.
    let anychecked = false;
    let anyunchecked = false;
    let allchecked = true;
    let allunchecked = true;
    for (const checkbox of document.body.querySelectorAll(".matsmb_checkmsg")) {
        if (checkbox.checked) {
            // -> checked
            anychecked = true;
            allunchecked = false;
        } else {
            // -> unchecked
            anyunchecked = true;
            allchecked = false;
        }
    }
    const checkall = document.getElementById('matsmb_checkall');

    // ?? Handle all checked, none checked, or anything between
    if (anychecked && anyunchecked) {
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
    matsmb_delete_cancel_bulk();

    // Activate or deactivate Reissue/Delete based on whether any is selected.
    const reissueBtn = document.getElementById('matsmb_reissue_bulk');
    const deleteBtn = document.getElementById('matsmb_delete_bulk');
    if (anychecked) {
        reissueBtn.classList.remove('matsmb_button_disabled')
        deleteBtn.classList.remove('matsmb_button_disabled')
    } else {
        reissueBtn.classList.add('matsmb_button_disabled')
        deleteBtn.classList.add('matsmb_button_disabled')
    }
}

function matsmb_reissue_or_delete_bulk(event, queueId, action) {

    // :: Find which messages
    const msgSysMsgIds = [];
    for (const checkbox of document.body.querySelectorAll(".matsmb_checkmsg")) {
        if (checkbox.checked) {
            msgSysMsgIds.push(checkbox.getAttribute("data-msgid"));
        }
    }

    console.log(msgSysMsgIds);

    let jsonPath = window.matsmb_json_path ? window.matsmb_json_path : window.location.pathname + "/json";
    let requestBody = {
        action: action,
        queueId: queueId,
        msgSysMsgIds: msgSysMsgIds
    };
    fetch(jsonPath, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    })
        .then(response => response.json())
        .then(data => console.log(data));
}


// ::: EXAMINE MESSAGE

// :: REISSUE / DELETE

function matsmb_reissue_single(event, queueId, msgSysMsgId) {
    matsmb_reissue_or_delete_single(event, queueId, msgSysMsgId, "reissue")
}

function matsmb_delete_propose_single(event) {
    // Gray out Delete
    document.getElementById("matsmb_delete_single").classList.add("matsmb_button_disabled");
    // Set Delete Confirm and Delete Cancel visible
    document.getElementById("matsmb_delete_confirm_single").classList.remove("matsmb_button_hidden");
    document.getElementById("matsmb_delete_cancel_single").classList.remove("matsmb_button_hidden");
}

function matsmb_delete_cancel_single(event) {
    // Set Delete Confirm and Delete Cancel hidden
    document.getElementById("matsmb_delete_confirm_single").classList.add("matsmb_button_hidden");
    document.getElementById("matsmb_delete_cancel_single").classList.add("matsmb_button_hidden");
    // Enable Delete
    document.getElementById("matsmb_delete_single").classList.remove("matsmb_button_disabled");
}

// Action for the "Confirm Delete" button made visible by clicking "Delete".
function matsmb_delete_confirmed_single(event, queueId, msgSysMsgId) {
    matsmb_reissue_or_delete_single(event, queueId, msgSysMsgId, "delete")
}


function matsmb_reissue_or_delete_single(event, queueId, msgSysMsgId, action) {
    let jsonPath = window.matsmb_json_path ? window.matsmb_json_path : window.location.pathname + "/json";
    let requestBody = {
        action: action,
        queueId: queueId,
        msgSysMsgIds: [msgSysMsgId]
    };
    fetch(jsonPath, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    })
        .then(response => response.json())
        .then(data => console.log(data));
}


// :: MATSTRACE CALL MODAL

let matsmb_activecallmodal = -1;

function matsmb_clearcallmodal(event) {
    // Clear the "underlay" for the modal
    console.log("Clear modal");
    console.log(event)
    console.log(event.target)
    let modalunderlay = document.getElementById("matsmb_callmodalunderlay");
    // Don't clear if the target is the modal, to enable interaction with it.
    if (event.target !== modalunderlay) {
        return;
    }
    console.log(modalunderlay);

    matsmb_clearcallmodal_noncond()

    return true;
}

function matsmb_clearcallmodal_noncond() {
    // Clear the modal underlay
    document.getElementById("matsmb_callmodalunderlay").classList.remove("matsmb_callmodalunderlay_visible")

    // Clear all call modals
    for (const modal of document.getElementsByClassName("matsbm_box_call_and_state_modal")) {
        modal.classList.remove("matsbm_box_call_and_state_modal_visible");
    }
    // Clear all call rows
    for (const row of document.body.querySelectorAll("#matsbm_table_matstrace tr")) {
        row.classList.remove("matsmb_row_active");
    }

    matsmb_activecallmodal = -1;
}

function matsbm_callmodal(event) {
    // Un-hide on the specific call modal
    let tr = event.target.closest("tr");
    let callno = tr.getAttribute("data-callno");
    let callmodal = document.getElementById("matsbm_callmodal_" + callno);
    console.log(callmodal);
    callmodal.classList.add("matsbm_box_call_and_state_modal_visible");

    // Un-hide the "underlay"
    let modalunderlay = document.getElementById("matsmb_callmodalunderlay");
    modalunderlay.classList.add("matsmb_callmodalunderlay_visible")

    // Make Call row active
    let callRow = document.getElementById("matsbm_callrow_" + callno);
    let processRow = document.getElementById("matsbm_processrow_" + callno);
    callRow.classList.add("matsmb_row_active")
    processRow.classList.add("matsmb_row_active")

    // Clear hover
    matsmb_hover_call_out();

    // Set the active call number
    matsmb_activecallmodal = callno;
}

// Modal Key listener: when modal is active.
document.addEventListener('keydown', (event) => {
    if (matsmb_activecallmodal < 0) {
        return;
    }
    var name = event.key;
    var code = event.code;
    console.log(`Key pressed ${name} \r\n Key code value: ${code}`);

    if (name === "Escape") {
        matsmb_clearcallmodal_noncond();
    }

    let currentCallModal = document.getElementById("matsbm_callmodal_" + matsmb_activecallmodal);
    let currentCallRow = document.getElementById("matsbm_callrow_" + matsmb_activecallmodal);
    let currentProcessRow = document.getElementById("matsbm_processrow_" + matsmb_activecallmodal);
    if (name === "ArrowUp") {
        matsmb_activecallmodal--;
        let nextCallModal = document.getElementById("matsbm_callmodal_" + matsmb_activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsmb_row_active")
            currentProcessRow.classList.remove("matsmb_row_active")
            let nextCallRow = document.getElementById("matsbm_callrow_" + matsmb_activecallmodal);
            let nextProcessRow = document.getElementById("matsbm_processrow_" + matsmb_activecallmodal);
            nextCallRow.classList.add("matsmb_row_active")
            nextProcessRow.classList.add("matsmb_row_active")
            // Handle the sticky header, so scroll above it
            if (matsmb_activecallmodal === 1) {
                let top = document.body.querySelector("#matsbm_table_matstrace thead");
                top.scrollIntoView({behavior: "smooth", block: "nearest"});
            } else {
                document.getElementById("matsbm_callrow_" + (matsmb_activecallmodal - 1))
                    .scrollIntoView({behavior: "smooth", block: "nearest"});
            }
        } else {
            // Back out
            matsmb_activecallmodal++;
        }
        event.preventDefault();
    }
    if (name === "ArrowDown") {
        matsmb_activecallmodal++;
        let nextCallModal = document.getElementById("matsbm_callmodal_" + matsmb_activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsmb_row_active")
            currentProcessRow.classList.remove("matsmb_row_active")
            let nextCallRow = document.getElementById("matsbm_callrow_" + matsmb_activecallmodal);
            let nextProcessRow = document.getElementById("matsbm_processrow_" + matsmb_activecallmodal);
            nextCallRow.classList.add("matsmb_row_active")
            nextProcessRow.classList.add("matsmb_row_active")
            nextCallRow.scrollIntoView({behavior: "smooth", block: "nearest"});
        } else {
            // Back out
            matsmb_activecallmodal--;
        }
        event.preventDefault();
    }
}, false);


// :: MATSTRACE HOVER

function matsmb_hover_call(event) {
    let tr = event.target.closest("tr");
    let callno = tr.getAttribute("data-callno");
    let callRow = document.getElementById("matsbm_callrow_" + callno);
    let processRow = document.getElementById("matsbm_processrow_" + callno);
    callRow.classList.add("matsmb_row_hover")
    processRow.classList.add("matsmb_row_hover")
}

function matsmb_hover_call_out() {
    for (const row of document.body.querySelectorAll("#matsbm_table_matstrace tr")) {
        row.classList.remove("matsmb_row_hover");
    }
}