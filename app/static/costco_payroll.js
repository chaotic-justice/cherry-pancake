const form = document.getElementById('payroll-form');
const fileInput = document.getElementById('file-input');
const fileWrapper = document.getElementById('file-wrapper');
const fileLabel = document.getElementById('file-label');
const buildBtn = document.getElementById('build-btn');

function handleFileChange(input) {
    const files = Array.from(input.files || []);
    const error = files.length > Number(form.dataset.maxFiles)
        ? `Too many files (max ${form.dataset.maxFiles})`
        : files.find(file => !file.name.toLowerCase().endsWith('.xlsx'))
            ? 'Every file must be an XLSX workbook'
            : files.find(file => file.size > Number(form.dataset.maxFileBytes))
                ? 'Every file must be 5 MB or smaller'
                : '';

    fileLabel.textContent = error || (files.length ? `${files.length} workbooks selected` : 'Choose Costco payroll workbooks');
    fileLabel.style.color = error ? 'var(--danger)' : '';
    fileWrapper.classList.toggle('active', files.length > 0 && !error);
    buildBtn.disabled = files.length === 0 || Boolean(error);
}
