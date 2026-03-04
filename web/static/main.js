document.addEventListener('DOMContentLoaded', () => {
    const generateBtn = document.getElementById('generate-btn');
    const queryInput = document.getElementById('query-input');
    const sqlOutput = document.getElementById('sql-output');
    const explanationContainer = document.getElementById('explanation-container');
    const safetyContainer = document.getElementById('safety-container');
    const resultsHead = document.getElementById('results-head');
    const resultsBody = document.getElementById('results-body');
    const resultsCount = document.getElementById('results-count');
    const executionTime = document.getElementById('execution-time');

    generateBtn.addEventListener('click', async () => {
        const query = queryInput.value.trim();
        if (!query) return;

        // Reset state
        generateBtn.disabled = true;
        document.getElementById('generate-btn-text').innerText = 'Generating...';
        sqlOutput.innerText = '-- Generating SQL...';
        explanationContainer.innerText = '';
        safetyContainer.innerText = '';
        resultsHead.innerHTML = '<tr><th class="py-3 px-6 text-xs font-bold text-slate-500 uppercase tracking-wider">Loading...</th></tr>';
        resultsBody.innerHTML = '';
        resultsCount.innerText = 'Loading...';

        const startTime = performance.now();

        try {
            // 1. Generate SQL
            const generateRes = await fetch('/api/generate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query })
            });

            if (!generateRes.ok) {
                const err = await generateRes.json();
                throw new Error(err.detail || 'Failed to generate SQL');
            }

            const data = await generateRes.json();
            sqlOutput.innerText = data.sql;
            explanationContainer.innerText = data.explanation;
            safetyContainer.innerText = data.safety_report;

            // 2. Execute SQL
            const executeRes = await fetch('/api/execute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql: data.sql })
            });

            if (!executeRes.ok) {
                const err = await executeRes.json();
                throw new Error(err.detail || 'Failed to execute SQL');
            }

            const resultData = await executeRes.json();
            const results = resultData.results;

            // 3. Render Results
            if (results && results.length > 0) {
                const columns = Object.keys(results[0]);

                // Headers
                resultsHead.innerHTML = `<tr class="bg-slate-50 border-b border-slate-200 sticky top-0">
                    ${columns.map(col => `<th class="py-3 px-6 text-xs font-bold text-slate-500 uppercase tracking-wider">${col}</th>`).join('')}
                </tr>`;

                // Body
                resultsBody.innerHTML = results.map(row => `
                    <tr class="hover:bg-slate-50 transition-colors">
                        ${columns.map(col => `<td class="py-3 px-6 text-sm text-slate-900">${row[col] !== null ? row[col] : 'NULL'}</td>`).join('')}
                    </tr>
                `).join('');

                resultsCount.innerText = `Showing ${results.length} results`;
            } else {
                resultsHead.innerHTML = '<tr><th class="py-3 px-6 text-xs font-bold text-slate-500 uppercase tracking-wider">No results found</th></tr>';
                resultsBody.innerHTML = '';
                resultsCount.innerText = 'Showing 0 results';
            }

        } catch (error) {
            console.error(error);
            sqlOutput.innerText = `-- Error: ${error.message}`;
            resultsHead.innerHTML = '<tr><th class="py-3 px-6 text-xs font-bold text-red-500 uppercase tracking-wider">Error executing query</th></tr>';
            resultsCount.innerText = 'Error';
        } finally {
            generateBtn.disabled = false;
            document.getElementById('generate-btn-text').innerText = 'Generate SQL';
            const endTime = performance.now();
            executionTime.innerText = ((endTime - startTime) / 1000).toFixed(2) + 's';
        }
    });
});

window.switchTab = function(tabName) {
    const tabs = ['sql', 'explanation', 'safety'];

    tabs.forEach(tab => {
        // Handle content visibility
        const container = document.getElementById(tab === 'sql' ? 'sql-container' : `${tab}-container`);
        if (tab === tabName) {
            container.classList.remove('hidden');
        } else {
            container.classList.add('hidden');
        }

        // Handle button styles
        const btn = document.getElementById(`tab-${tab}`);
        if (tab === tabName) {
            btn.className = 'px-3 py-1 text-xs font-medium rounded-md bg-white text-slate-900 shadow-sm';
        } else {
            btn.className = 'px-3 py-1 text-xs font-medium rounded-md text-slate-500 hover:text-slate-900';
        }
    });
};

window.copyToClipboard = function(elementId) {
    const text = document.getElementById(elementId).innerText;
    navigator.clipboard.writeText(text).then(() => {
        // Could add a toast notification here
        console.log('Copied to clipboard');
    });
};