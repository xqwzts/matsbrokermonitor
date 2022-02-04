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

function matsbm_noclick() {
    return false;
}

function matsmb_clearcallmodal() {
    // Clear the "underlay" for the modal
    let modalunderlay = document.getElementById("matsmb_callmodalunderlay");
    console.log(modalunderlay);
    modalunderlay.classList.remove("matsmb_callmodalunderlay_visible")

    // Clear all modals
    for (const modal of document.getElementsByClassName("matsbm_box_call_and_state_modal")) {
        modal.classList.remove("matsbm_box_call_and_state_modal_visible");
    }

    return true;
}

function matsbm_call(event) {
    console.log("Triggered!");
    console.log(event);

    // Un-hide on the specific call modal
    let td = event.target.closest("td");
    let callno = td.getAttribute("data-callno");
    let callmodal = document.getElementById("matsbm_call_"+callno);
    console.log(callmodal);
    callmodal.classList.add("matsbm_box_call_and_state_modal_visible");

    // Un-hide the "underlay"
    let modalunderlay = document.getElementById("matsmb_callmodalunderlay");
    modalunderlay.classList.add("matsmb_callmodalunderlay_visible")

}