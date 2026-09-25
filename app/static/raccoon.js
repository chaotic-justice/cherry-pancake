function wireDropZone(inputId, zoneId, listId) {
        const input = document.querySelector(inputId);
        const zone = document.querySelector(zoneId);
        const list = document.querySelector(listId);
        const showFiles = () => list.replaceChildren(...Array.from(input.files, file => {
            const item = document.createElement("li");
            item.textContent = file.name;
            return item;
        }));
        ["dragenter", "dragover", "dragleave", "drop"].forEach(name => zone.addEventListener(name, event => event.preventDefault()));
        ["dragenter", "dragover"].forEach(name => zone.addEventListener(name, () => zone.classList.add("dragging")));
        ["dragleave", "drop"].forEach(name => zone.addEventListener(name, () => zone.classList.remove("dragging")));
        zone.addEventListener("drop", event => { input.files = event.dataTransfer.files; showFiles(); });
        input.addEventListener("change", showFiles);
    }
    wireDropZone("#files", "#recon-zone", "#recon-list");
    wireDropZone("#ar-files", "#ar-zone", "#ar-list");

    const submit = document.querySelector("#submit");
    submit.form.addEventListener("submit", () => {
        submit.disabled = true;
        submit.textContent = "Reconciling…";
    });

    const filters = document.querySelectorAll(".filter");
    function filterRows(value) {
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
    }
    filters.forEach(button => button.addEventListener("click", () => {
        filters.forEach(filter => filter.setAttribute("aria-pressed", String(filter === button)));
        filterRows(button.dataset.filter);
    }));
    if (filters.length) filterRows("attention");
