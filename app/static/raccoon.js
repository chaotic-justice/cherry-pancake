function wireDropZone(inputId, zoneId, listId) {
    const input = document.querySelector(inputId);
    const zone = document.querySelector(zoneId);
    const list = document.querySelector(listId);
    if (!input || !zone || !list) return;

    const showFiles = () => list.replaceChildren(...Array.from(input.files, file => {
        const item = document.createElement("li");
        item.textContent = file.name;
        return item;
    }));

    ["dragenter", "dragover", "dragleave", "drop"].forEach(name => {
        zone.addEventListener(name, event => event.preventDefault());
    });
    ["dragenter", "dragover"].forEach(name => {
        zone.addEventListener(name, () => zone.classList.add("dragging"));
    });
    ["dragleave", "drop"].forEach(name => {
        zone.addEventListener(name, () => zone.classList.remove("dragging"));
    });
    zone.addEventListener("drop", event => {
        input.files = event.dataTransfer.files;
        showFiles();
    });
    input.addEventListener("change", showFiles);
}

function wireSubmission() {
    const form = document.querySelector("#raccoon-form");
    if (!form) return;

    form.addEventListener("submit", async event => {
        event.preventDefault();

        const submit = form.querySelector("#submit");
        const status = document.querySelector("#request-status");
        submit.disabled = true;
        submit.textContent = "Reconciling…";
        form.setAttribute("aria-busy", "true");
        status.textContent = "Uploading files and reconciling. This may take a moment.";

        try {
            const response = await fetch(form.action, {
                method: "POST",
                body: new FormData(form),
            });
            const nextDocument = new DOMParser().parseFromString(
                await response.text(),
                "text/html",
            );
            const nextMain = nextDocument.querySelector("main");
            if (!nextMain) throw new Error("Invalid server response");

            document.querySelector("main").replaceWith(nextMain);
            document.title = nextDocument.title;
            initializeRaccoon();

            const focusTarget = document.querySelector("[role='alert'], #results-heading");
            if (focusTarget) {
                focusTarget.tabIndex = -1;
                focusTarget.focus();
            }
        } catch {
            submit.disabled = false;
            submit.textContent = "Reconcile and review";
            form.removeAttribute("aria-busy");
            status.textContent = "Could not reach the server. Check your connection and try again.";
        }
    });
}

function wireFilters() {
    const filters = document.querySelectorAll(".filter");
    const filterRows = value => {
        document.querySelectorAll("details").forEach(section => {
            let visible = 0;
            const rows = section.querySelectorAll("tbody tr");
            rows.forEach(row => {
                const show = value === "all"
                    || (value === "attention" && ["needs review", "warning"].includes(row.dataset.status))
                    || row.dataset.status === value;
                row.hidden = !show;
                visible += show ? 1 : 0;
            });
            const empty = section.querySelector(".empty-filter");
            if (empty) empty.classList.toggle("visible", visible === 0);
        });
    };

    filters.forEach(button => button.addEventListener("click", () => {
        filters.forEach(filter => {
            filter.setAttribute("aria-pressed", String(filter === button));
        });
        filterRows(button.dataset.filter);
    }));
    if (filters.length) filterRows("attention");
}

function initializeRaccoon() {
    wireDropZone("#files", "#recon-zone", "#recon-list");
    wireDropZone("#ar-files", "#ar-zone", "#ar-list");
    wireSubmission();
    wireFilters();
}

initializeRaccoon();
