/**
 * Cliente Paxos - Scripts para a interface web
 */
document.addEventListener('DOMContentLoaded', function() {
    // Inicializa tabs
    initTabs();
    
    // Inicializa filtros de tabelas
    initTableFilters();
    
    // Inicializa modal para detalhes da operação
    initOperationModal();
    
    // Inicializa formulário de operação manual
    initManualOperationForm();
    
    // Inicializa formulário de edição de recurso
    initResourceEditForm();
    
    // Inicializa botão de toggle cliente (iniciar/parar)
    initClientToggle();
    
    // Exibe timestamps em formato legível
    formatTimestamps();
});

/**
 * Inicializa sistema de abas (tabs)
 */
function initTabs() {
    const tabButtons = document.querySelectorAll('.tab-button');
    if (tabButtons.length === 0) return;
    
    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            // Remove classe active de todos os botões e painéis
            document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
            
            // Adiciona classe active ao botão clicado e ao painel correspondente
            button.classList.add('active');
            const tabId = button.getAttribute('data-tab');
            document.getElementById(tabId).classList.add('active');
        });
    });
}

/**
 * Inicializa filtros para tabelas
 */
function initTableFilters() {
    // Filtro de status para tabela de operações
    const statusFilter = document.getElementById('status-filter');
    if (statusFilter) {
        statusFilter.addEventListener('change', function() {
            const status = this.value;
            const rows = document.querySelectorAll('#operations-table tbody tr');
            
            rows.forEach(row => {
                if (status === 'all' || row.getAttribute('data-status') === status) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            });
        });
    }
    
    // Filtro de ordenação para tabela de operações
    const sortOrder = document.getElementById('sort-order');
    if (sortOrder) {
        sortOrder.addEventListener('change', function() {
            const order = this.value;
            const tbody = document.querySelector('#operations-table tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            
            rows.sort((a, b) => {
                const aStart = parseInt(a.cells[2].innerText);
                const bStart = parseInt(b.cells[2].innerText);
                
                if (order === 'newest') {
                    return bStart - aStart;
                } else {
                    return aStart - bStart;
                }
            });
            
            // Limpa a tabela
            tbody.innerHTML = '';
            
            // Adiciona as linhas ordenadas
            rows.forEach(row => tbody.appendChild(row));
        });
    }
    
    // Filtro de nível para tabela de logs
    const levelFilter = document.getElementById('level-filter');
    if (levelFilter) {
        levelFilter.addEventListener('change', function() {
            const level = this.value;
            const rows = document.querySelectorAll('#logs-table tbody tr, #system-logs-table tbody tr');
            
            rows.forEach(row => {
                if (level === 'all' || row.getAttribute('data-level') === level) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            });
        });
    }
    
    // Filtro de componente para tabela de logs do sistema
    const componentFilter = document.getElementById('component-filter');
    if (componentFilter) {
        componentFilter.addEventListener('change', function() {
            const component = this.value;
            const rows = document.querySelectorAll('#system-logs-table tbody tr');
            
            rows.forEach(row => {
                if (component === 'all' || row.getAttribute('data-component') === component) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            });
        });
    }
    
    // Filtro de busca para tabelas de logs
    const searchInput = document.getElementById('search');
    if (searchInput) {
        searchInput.addEventListener('input', function() {
            const searchTerm = this.value.toLowerCase();
            const logRows = document.querySelectorAll('#logs-table tbody tr, #system-logs-table tbody tr');
            
            logRows.forEach(row => {
                const text = row.textContent.toLowerCase();
                if (text.includes(searchTerm)) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            });
        });
    }
}

/**
 * Inicializa modal para exibir detalhes da operação
 */
function initOperationModal() {
    const modal = document.getElementById('operation-details-modal');
    if (!modal) return;
    
    // Obtém elementos do modal
    const modalContent = document.querySelector('.modal-content');
    const closeBtn = document.querySelector('.close');
    const detailsContainer = document.getElementById('operation-details');
    
    // Fecha o modal ao clicar no X
    closeBtn.addEventListener('click', () => {
        modal.style.display = 'none';
    });
    
    // Fecha o modal ao clicar fora dele
    window.addEventListener('click', (event) => {
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });
    
    // Adiciona evento aos botões de detalhes
    const detailButtons = document.querySelectorAll('.view-details');
    detailButtons.forEach(button => {
        button.addEventListener('click', async () => {
            const operationId = button.getAttribute('data-id');
            
            try {
                // Obtém detalhes da operação
                const response = await fetch(`/operation/${operationId}`);
                if (!response.ok) {
                    throw new Error(`Erro ao obter detalhes: ${response.status}`);
                }
                
                const operation = await response.json();
                
                // Formata os detalhes
                let html = '<div class="operation-details">';
                html += `<h3>Operação #${operation.id}</h3>`;
                html += `<p><strong>Status:</strong> <span class="status-${operation.status}">${operation.status}</span></p>`;
                
                if (operation.instance_id) {
                    html += `<p><strong>ID da Instância:</strong> ${operation.instance_id}</p>`;
                }
                
                if (operation.start_time) {
                    const startDate = new Date(operation.start_time * 1000);
                    html += `<p><strong>Início:</strong> ${startDate.toLocaleString()}</p>`;
                }
                
                if (operation.end_time) {
                    const endDate = new Date(operation.end_time * 1000);
                    html += `<p><strong>Fim:</strong> ${endDate.toLocaleString()}</p>`;
                }
                
                if (operation.latency) {
                    html += `<p><strong>Latência:</strong> ${operation.latency.toFixed(2)}s</p>`;
                }
                
                if (operation.retries) {
                    html += `<p><strong>Tentativas:</strong> ${operation.retries + 1}</p>`;
                }
                
                if (operation.error) {
                    html += `<p><strong>Erro:</strong> ${operation.error}</p>`;
                }
                
                // Adiciona payload da operação
                if (operation.payload) {
                    html += '<h4>Payload</h4>';
                    html += `<pre>${JSON.stringify(operation.payload, null, 2)}</pre>`;
                }
                
                // Adiciona notificação do learner (se disponível)
                if (operation.notification) {
                    html += '<h4>Notificação do Learner</h4>';
                    html += `<pre>${JSON.stringify(operation.notification, null, 2)}</pre>`;
                }
                
                html += '</div>';
                
                // Atualiza o conteúdo do modal
                detailsContainer.innerHTML = html;
                
                // Exibe o modal
                modal.style.display = 'block';
                
            } catch (error) {
                console.error('Erro ao carregar detalhes da operação:', error);
                alert(`Erro ao carregar detalhes: ${error.message}`);
            }
        });
    });
}

/**
 * Inicializa formulário de operação manual
 */
function initManualOperationForm() {
    const form = document.getElementById('manual-operation-form');
    if (!form) return;
    
    form.addEventListener('submit', async function(event) {
        event.preventDefault();
        
        const dataInput = document.getElementById('operation-data');
        const data = dataInput.value.trim();
        
        if (!data) {
            alert('Por favor, digite os dados da operação.');
            return;
        }
        
        try {
            const response = await fetch('/api/operation', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            
            if (!response.ok) {
                throw new Error(`Erro: ${response.status}`);
            }
            
            const result = await response.json();
            
            // Exibe resultado
            const resultContainer = document.getElementById('operation-result');
            const resultContent = document.getElementById('operation-result-content');
            
            resultContent.innerHTML = `
                <p>Operação iniciada com sucesso!</p>
                <p><strong>ID da Operação:</strong> ${result.operation_id}</p>
                <p>A página será atualizada em 5 segundos para mostrar o resultado.</p>
            `;
            
            resultContainer.classList.remove('hidden');
            
            // Reseta o formulário
            form.reset();
            
            // Atualiza a página após 5 segundos
            setTimeout(() => {
                window.location.reload();
            }, 5000);
            
        } catch (error) {
            console.error('Erro ao enviar operação:', error);
            alert(`Erro ao enviar operação: ${error.message}`);
        }
    });
}

/**
 * Inicializa formulário de edição de recurso
 */
function initResourceEditForm() {
    const form = document.getElementById('resource-edit-form');
    if (!form) return;
    
    form.addEventListener('submit', async function(event) {
        event.preventDefault();
        
        const dataInput = document.getElementById('resource-data');
        const data = dataInput.value.trim();
        
        if (!data) {
            alert('Por favor, digite o conteúdo do recurso.');
            return;
        }
        
        try {
            const response = await fetch('/api/operation', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            
            if (!response.ok) {
                throw new Error(`Erro: ${response.status}`);
            }
            
            const result = await response.json();
            
            // Exibe resultado
            const resultContainer = document.getElementById('edit-result');
            const resultContent = document.getElementById('edit-result-content');
            
            resultContent.innerHTML = `
                <p>Operação de edição iniciada com sucesso!</p>
                <p><strong>ID da Operação:</strong> ${result.operation_id}</p>
                <p>A página será atualizada em 10 segundos para mostrar o resultado.</p>
            `;
            
            resultContainer.classList.remove('hidden');
            
            // Atualiza a página após 10 segundos
            setTimeout(() => {
                window.location.reload();
            }, 10000);
            
        } catch (error) {
            console.error('Erro ao editar recurso:', error);
            alert(`Erro ao editar recurso: ${error.message}`);
        }
    });
}

/**
 * Inicializa botão de toggle cliente (iniciar/parar)
 */
function initClientToggle() {
    const toggleButton = document.getElementById('toggle-client');
    if (!toggleButton) return;
    
    toggleButton.addEventListener('click', async () => {
        const action = toggleButton.innerText === 'Iniciar' ? 'start' : 'stop';
        
        try {
            const response = await fetch(`/api/${action}`, {
                method: 'POST'
            });
            
            if (!response.ok) {
                throw new Error(`Erro: ${response.status}`);
            }
            
            // Atualiza a página
            window.location.reload();
            
        } catch (error) {
            console.error(`Erro ao ${action === 'start' ? 'iniciar' : 'parar'} cliente:`, error);
            alert(`Erro: ${error.message}`);
        }
    });
}

/**
 * Formata timestamps para formato legível
 */
function formatTimestamps() {
    document.querySelectorAll('td:nth-child(1)').forEach(cell => {
        const timestamp = parseInt(cell.innerText.trim());
        if (!isNaN(timestamp) && timestamp > 1000000000) {
            const date = new Date(timestamp);
            if (date.toString() !== 'Invalid Date') {
                cell.innerText = date.toLocaleString();
            }
        }
    });
}