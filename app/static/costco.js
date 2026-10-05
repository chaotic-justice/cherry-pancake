const analysisForm = document.getElementById('analysis-form');
const pdfInput = document.getElementById('pdf-input');
const storeInput = document.getElementById('store-input');
const downloadBtn = document.getElementById('download-btn');

const maxReportFiles = Number(analysisForm.dataset.maxReportFiles);
const maxFileBytes = Number(analysisForm.dataset.maxFileBytes);
const maxFileSizeMb = Number(analysisForm.dataset.maxFileSizeMb);
const reportSuffixes = analysisForm.dataset.reportSuffixes.split(',');
const storeUpdateSuffixes = analysisForm.dataset.storeUpdateSuffixes.split(',');

function firstOversizedFile(files) {
    return Array.from(files || []).find(file => file.size > maxFileBytes);
}

function selectionError(input) {
    const files = input.files || [];
    if (input === pdfInput && files.length > maxReportFiles) {
        return `Too many files (max ${maxReportFiles})`;
    }

    const allowedSuffixes = input === pdfInput ? reportSuffixes : storeUpdateSuffixes;
    const unexpectedFile = Array.from(files).find(file => {
        const filename = file.name.toLowerCase();
        return !allowedSuffixes.some(suffix => filename.endsWith(suffix));
    });
    if (unexpectedFile) {
        return input === pdfInput
            ? `${unexpectedFile.name} must be a PDF`
            : `${unexpectedFile.name} must be CSV, XLS, or XLSX`;
    }

    const oversizedFile = firstOversizedFile(files);
    if (oversizedFile) {
        return `${oversizedFile.name} is larger than ${maxFileSizeMb} MB`;
    }

    return '';
}

function updateSelectionLabel(input, label, wrapper) {
    const error = selectionError(input);
    if (error) {
        label.textContent = error;
        label.style.color = 'var(--danger)';
        wrapper.classList.remove('active');
        return;
    }

    if (input.files && input.files.length > 0) {
        label.textContent = input.files.length === 1
            ? input.files[0].name
            : `${input.files.length} files selected`;
        label.style.color = '';
        wrapper.classList.add('active');
        return;
    }

    label.textContent = input === storeInput
        ? 'Choose an updated store list'
        : 'Choose Costco payment PDFs';
    label.style.color = '';
    wrapper.classList.remove('active');
}

function updateSubmitState() {
    const pdfCount = pdfInput.files ? pdfInput.files.length : 0;
    const reportsValid = pdfCount > 0 && !selectionError(pdfInput);
    const storeUpdateValid = !selectionError(storeInput);
    downloadBtn.disabled = !(reportsValid && storeUpdateValid);
}

function handleFileChange(input, labelId, wrapperId) {
    const label = document.getElementById(labelId);
    const wrapper = document.getElementById(wrapperId);
    updateSelectionLabel(input, label, wrapper);
    updateSubmitState();
}
