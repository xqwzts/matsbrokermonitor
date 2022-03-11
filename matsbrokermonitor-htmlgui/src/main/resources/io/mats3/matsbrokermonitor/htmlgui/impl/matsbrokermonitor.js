function matsbm_noclick(event) {
    event.preventDefault();
    return false;
}

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

function matsmb_reissue(event, queueId, msgSysMsgId) {
    matsmb_reissue_delete(event, queueId, msgSysMsgId, "reissue")
}
function matsmb_delete_confirmed(event, queueId, msgSysMsgId) {
    matsmb_reissue_delete(event, queueId, msgSysMsgId, "delete")
}

function matsmb_delete_propose(event) {
    // Gray out Delete
    document.getElementById("matsmb_delete").classList.add("matsmb_button_delete_disabled");
    // Set Delete Confirm and Delete Cancel visible
    document.getElementById("matsmb_delete_confirm").classList.remove("matsmb_button_hidden");
    document.getElementById("matsmb_delete_cancel").classList.remove("matsmb_button_hidden");
}

function matsmb_delete_cancel(event) {
    // Set Delete Confirm and Delete Cancel hidden
    document.getElementById("matsmb_delete_confirm").classList.add("matsmb_button_hidden");
    document.getElementById("matsmb_delete_cancel").classList.add("matsmb_button_hidden");
    // Enable Delete
    document.getElementById("matsmb_delete").classList.remove("matsmb_button_delete_disabled");
}



function matsmb_reissue_delete(event, queueId, msgSysMsgId, action) {
    let jsonPath = window.matsmb_json_path ? window.matsmb_json_path : window.location.pathname + "/json";
    let requestBody = {
        action: action,
        queueId: queueId,
        msgSysMsgId: msgSysMsgId
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
    console.log("Triggered!");
    console.log(event);
    console.log(event.target)

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

    // Set the active call number
    matsmb_activecallmodal = callno;
}

function matsmb_hover_call(event) {
    let tr = event.target.closest("tr");
    let callno = tr.getAttribute("data-callno");
    let callRow = document.getElementById("matsbm_callrow_" + callno);
    let processRow = document.getElementById("matsbm_processrow_" + callno);
    callRow.classList.add("matsmb_row_hover")
    processRow.classList.add("matsmb_row_hover")
}

function matsmb_hover_call_out(event) {
    for (const row of document.body.querySelectorAll("#matsbm_table_matstrace tr")) {
        row.classList.remove("matsmb_row_hover");
    }
}

// Key listener when modal is active.
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