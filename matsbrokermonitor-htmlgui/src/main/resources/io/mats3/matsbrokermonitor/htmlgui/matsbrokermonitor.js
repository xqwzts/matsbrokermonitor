window.onload = function () {
    const modal = document.querySelector(".modal");
    const trigger = document.querySelector(".trigger");
    const closeButton = document.querySelector(".close-button");

    function toggleModal() {
        modal.classList.toggle("show-modal");
    }

    function windowOnClick(event) {
        if (event.target === modal) {
            toggleModal();
        }
    }

    trigger.addEventListener("click", toggleModal);
    closeButton.addEventListener("click", toggleModal);
    window.addEventListener("click", windowOnClick);
};

function matsbm_noclick(event) {
    event.preventDefault();
    return false;
}

let activecallmodal = -1;

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
    modalunderlay.classList.remove("matsmb_callmodalunderlay_visible")

    // Clear all call modals
    for (const modal of document.getElementsByClassName("matsbm_box_call_and_state_modal")) {
        modal.classList.remove("matsbm_box_call_and_state_modal_visible");
    }
    // Clear all call rows
    for (const row of document.body.querySelectorAll("#matsbm_table_matstrace tr")) {
        row.classList.remove("matsmb_callrow_active");
    }

    activecallmodal = -1;

    return true;
}

function matsbm_callmodal(event) {
    console.log("Triggered!");
    console.log(event);
    console.log(event.target)

    // Un-hide on the specific call modal
    let td = event.target.closest("td");
    let callno = td.getAttribute("data-callno");
    let callmodal = document.getElementById("matsbm_callmodal_" + callno);
    console.log(callmodal);
    callmodal.classList.add("matsbm_box_call_and_state_modal_visible");

    // Un-hide the "underlay"
    let modalunderlay = document.getElementById("matsmb_callmodalunderlay");
    modalunderlay.classList.add("matsmb_callmodalunderlay_visible")

    // Make Call row active
    let callRow = document.getElementById("matsbm_callrow_" + callno);
    callRow.classList.add("matsmb_callrow_active")

    // Set the active call number
    activecallmodal = callno;
}

document.addEventListener('keydown', (event) => {
    if (activecallmodal < 0) {
        return;
    }
    var name = event.key;
    var code = event.code;
    console.log(`Key pressed ${name} \r\n Key code value: ${code}`);

    let currentCallModal = document.getElementById("matsbm_callmodal_" + activecallmodal);
    let currentCallRow = document.getElementById("matsbm_callrow_" + activecallmodal);
    if (name === "ArrowUp") {
        activecallmodal--;
        let nextCallModal = document.getElementById("matsbm_callmodal_" + activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsmb_callrow_active")
            let nextCallRow = document.getElementById("matsbm_callrow_" + activecallmodal);
            nextCallRow.classList.add("matsmb_callrow_active")
            // Handle the sticky header, so scroll above it
            if (activecallmodal === 1) {
                let top = document.body.querySelector("#matsbm_table_matstrace thead");
                console.log(top);
                top.scrollIntoView({behavior: "smooth", block: "nearest"});
            } else {
                document.getElementById("matsbm_callrow_" + (activecallmodal - 1))
                    .scrollIntoView({behavior: "smooth", block: "nearest"});
            }
        } else {
            // Back out
            activecallmodal++;
        }
        event.preventDefault();
    }
    if (name === "ArrowDown") {
        activecallmodal++;
        let nextCallModal = document.getElementById("matsbm_callmodal_" + activecallmodal);
        if (nextCallModal) {
            // Call modal
            currentCallModal.classList.remove("matsbm_box_call_and_state_modal_visible");
            nextCallModal.classList.add("matsbm_box_call_and_state_modal_visible");
            // Call row
            currentCallRow.classList.remove("matsmb_callrow_active")
            let nextCallRow = document.getElementById("matsbm_callrow_" + activecallmodal);
            nextCallRow.classList.add("matsmb_callrow_active")
            nextCallRow.scrollIntoView({behavior: "smooth", block: "nearest"});
        } else {
            // Back out
            activecallmodal--;
        }
        event.preventDefault();
    }
}, false);