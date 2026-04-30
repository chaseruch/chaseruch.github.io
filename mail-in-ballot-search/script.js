let ballotData = [];

document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.getElementById('searchInput');
    const tableBody = document.getElementById('tableBody');
    const noResults = document.getElementById('noResults');
    const table = document.getElementById('resultsTable');

    // Fetch and parse the CSV data
    fetch('cleanmail.csv')
        .then(response => {
            if (!response.ok) throw new Error('Network response was not ok');
            return response.text();
        })
        .then(csvText => {
            ballotData = parseCSV(csvText);
            // Sort data alphabetically by county name by default
            ballotData.sort((a, b) => a.name.localeCompare(b.name));
            renderTable(ballotData);
        })
        .catch(error => {
            console.error('Error fetching CSV:', error);
            noResults.textContent = 'Error loading data. Please try again later.';
            table.classList.add('hidden');
            noResults.classList.remove('hidden');
        });

    // Handle search input
    searchInput.addEventListener('input', (e) => {
        const searchTerm = e.target.value.toLowerCase().trim();
        
        if (searchTerm === '') {
            renderTable(ballotData);
            return;
        }

        const filteredData = ballotData.filter(row => 
            row.name.toLowerCase().includes(searchTerm)
        );

        renderTable(filteredData);
    });

    function parseCSV(csvText) {
        const lines = csvText.trim().split('\n');
        const data = [];
        
        // Skip header (index 0)
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i].trim();
            if (!line) continue;
            
            const columns = line.split(',');
            
            if (columns.length >= 7) {
                // Ensure correct formatting
                const turnout = parseFloat(columns[3]);
                const mailPercent = parseFloat(columns[4]);

                data.push({
                    name: columns[0].trim(),
                    f1a: parseInt(columns[1], 10).toLocaleString(),
                    mailVotes: parseInt(columns[2], 10).toLocaleString(),
                    turnout: !isNaN(turnout) ? turnout.toFixed(2) + '%' : 'N/A',
                    mailPercent: !isNaN(mailPercent) ? mailPercent.toFixed(2) + '%' : 'N/A',
                    rejected: parseInt(columns[5], 10).toLocaleString(),
                    rejectedLate: parseInt(columns[6], 10).toLocaleString()
                });
            }
        }
        return data;
    }

    function renderTable(data) {
        tableBody.innerHTML = '';
        
        if (data.length === 0) {
            table.classList.add('hidden');
            noResults.classList.remove('hidden');
            return;
        }

        table.classList.remove('hidden');
        noResults.classList.add('hidden');

        const fragment = document.createDocumentFragment();

        data.forEach(row => {
            const tr = document.createElement('tr');
            
            tr.innerHTML = `
                <td style="font-weight: 600; color: #1e293b;">${row.name}</td>
                <td>${row.f1a}</td>
                <td>${row.mailVotes}</td>
                <td>
                    <span class="pill pill-blue">${row.turnout}</span>
                </td>
                <td>
                    <span class="pill pill-pink">${row.mailPercent}</span>
                </td>
                <td>${row.rejected}</td>
                <td>${row.rejectedLate}</td>
            `;
            
            fragment.appendChild(tr);
        });

        tableBody.appendChild(fragment);
    }
});
