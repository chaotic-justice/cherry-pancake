const fileInput = document.getElementById('file-input');
    const analyzeBtn = document.getElementById('analyze-btn');
    const fileWrapper = document.getElementById('file-wrapper');
    const fileLabel = document.getElementById('file-label');

    function handleFileChange(input) {
        if (input.files && input.files.length > 0) {
            fileLabel.textContent = input.files[0].name;
            fileWrapper.classList.add('active');
            analyzeBtn.disabled = false;
        } else {
            fileLabel.textContent = 'Choose the sales workbook';
            fileWrapper.classList.remove('active');
            analyzeBtn.disabled = true;
        }
    }
