const pdfInput = document.getElementById('pdf-input');
    const downloadBtn = document.getElementById('download-btn');

    function handleFileChange(input, labelId, wrapperId) {
        const label = document.getElementById(labelId);
        const wrapper = document.getElementById(wrapperId);

        if (input.files && input.files.length > 0) {
            if (input.id === 'pdf-input' && input.files.length > 30) {
                label.textContent = 'Too many files (max 30)';
                label.style.color = '#ef4444';
                wrapper.classList.remove('active');
            } else {
                label.textContent = input.files.length === 1 ? input.files[0].name : input.files.length + ' files selected';
                label.style.color = '';
                wrapper.classList.add('active');
            }
        } else {
            label.textContent = input.id === 'store-input' ? 'Upload an updated store list (optional)' : 'Select PDF files (max 30)';
            label.style.color = '';
            wrapper.classList.remove('active');
        }

        const pdfCount = pdfInput.files ? pdfInput.files.length : 0;
        const validPdfCount = pdfCount > 0 && pdfCount <= 30;

        downloadBtn.disabled = !validPdfCount;
    }
