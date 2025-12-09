const API_BASE_URL = 'http://localhost:8000';

// Função para abrir abas
function abrirAba(evt, abaNome) {
    // Esconder todos os conteúdos de abas
    const tabContents = document.getElementsByClassName('tab-content');
    for (let i = 0; i < tabContents.length; i++) {
        tabContents[i].classList.remove('active');
    }

    // Remover classe active de todos os botões
    const tabButtons = document.getElementsByClassName('tab-button');
    for (let i = 0; i < tabButtons.length; i++) {
        tabButtons[i].classList.remove('active');
    }

    // Mostrar a aba atual e adicionar classe active ao botão
    document.getElementById(abaNome).classList.add('active');
    evt.currentTarget.classList.add('active');

    // Carregar dados da aba
    carregarDadosAba(abaNome);
}

// Função para carregar dados da aba
function carregarDadosAba(abaNome) {
    switch(abaNome) {
        case 'clientes':
            carregarClientes();
            break;
        case 'amigos':
            carregarAmigos();
            break;
        case 'compras':
            carregarCompras();
            break;
        case 'recomendacoes':
            carregarRecomendacoes();
            break;
    }
}

// Função para sincronizar dados
async function sincronizarDados() {
    const syncBtn = document.getElementById('syncBtn');
    const syncStatus = document.getElementById('syncStatus');
    
    syncBtn.disabled = true;
    syncBtn.textContent = '⏳ Sincronizando...';
    syncStatus.className = 'status-message info';
    syncStatus.textContent = 'Sincronizando dados das bases... Isso pode levar alguns segundos.';

    try {
        const response = await fetch(`${API_BASE_URL}/api/sync_data`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        const data = await response.json();

        if (response.ok) {
            syncStatus.className = 'status-message success';
            syncStatus.textContent = `✅ ${data.message} - ${data.clientes_processados} clientes processados.`;
            
            // Recarregar todas as abas
            const abaAtiva = document.querySelector('.tab-content.active').id;
            carregarDadosAba(abaAtiva);
        } else {
            throw new Error(data.detail || 'Erro ao sincronizar');
        }
    } catch (error) {
        syncStatus.className = 'status-message error';
        syncStatus.textContent = `❌ Erro: ${error.message}`;
    } finally {
        syncBtn.disabled = false;
        syncBtn.textContent = '🔄 Sincronizar/Atualizar Bases';
    }
}

// Função para carregar clientes
async function carregarClientes() {
    const content = document.getElementById('clientesContent');
    content.innerHTML = '<p class="loading">Carregando clientes...</p>';

    try {
        const response = await fetch(`${API_BASE_URL}/api/clientes`);
        const data = await response.json();

        if (data.status === 'success') {
            let html = `
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-value">${data.total}</div>
                        <div class="stat-label">Total de Clientes</div>
                    </div>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Nome</th>
                            <th>CPF</th>
                            <th>Cidade/UF</th>
                            <th>Email</th>
                            <th>Compras</th>
                            <th>Amigos</th>
                            <th>Interesses</th>
                        </tr>
                    </thead>
                    <tbody>
            `;

            data.clientes.forEach(cliente => {
                html += `
                    <tr>
                        <td>${cliente.id}</td>
                        <td><strong>${cliente.nome}</strong></td>
                        <td>${cliente.cpf}</td>
                        <td>${cliente.cidade}/${cliente.uf}</td>
                        <td>${cliente.email}</td>
                        <td><span class="badge badge-info">${cliente.num_compras}</span></td>
                        <td><span class="badge badge-primary">${cliente.num_amigos}</span></td>
                        <td>
                            <div class="interesses-list">
                                ${cliente.interesses && cliente.interesses.length
                                    ? cliente.interesses.map(interesse => `<span class="badge badge-success">${interesse}</span>`).join(' ')
                                    : '<span style="color: #6c757d;">Nenhum interesse cadastrado</span>'
                                }
                            </div>
                        </td>
                    </tr>
                `;
            });

            html += `
                    </tbody>
                </table>
            `;

            content.innerHTML = html;
        } else {
            throw new Error('Erro ao carregar clientes');
        }
    } catch (error) {
        content.innerHTML = `<div class="error-message">Erro ao carregar clientes: ${error.message}</div>`;
    }
}

// Função para carregar amigos
async function carregarAmigos() {
    const content = document.getElementById('amigosContent');
    content.innerHTML = '<p class="loading">Carregando rede de amizades...</p>';

    try {
        const response = await fetch(`${API_BASE_URL}/api/clientes/amigos`);
        const data = await response.json();

        if (data.status === 'success') {
            let html = `
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-value">${data.total}</div>
                        <div class="stat-label">Clientes na Rede</div>
                    </div>
                </div>
            `;

            data.clientes_amigos.forEach(item => {
                html += `
                    <div class="card">
                        <div class="card-header">
                            👤 ${item.cliente.nome} (ID: ${item.cliente.id})
                        </div>
                        <div class="card-body">
                            <div class="info-row">
                                <span class="info-label">CPF:</span>
                                <span class="info-value">${item.cliente.cpf}</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Total de Amigos:</span>
                                <span class="info-value"><span class="badge badge-primary">${item.total_amigos}</span></span>
                            </div>
                            ${item.amigos.length > 0 ? `
                                <div style="margin-top: 15px;">
                                    <strong>Amigos:</strong>
                                    <div class="amigos-list">
                            ` : '<p style="margin-top: 15px; color: #6c757d;">Nenhum amigo cadastrado.</p>'}
                `;

                item.amigos.forEach(amigo => {
                    html += `
                        <div class="amigo-item">
                            ${amigo.nome} (ID: ${amigo.id})
                        </div>
                    `;
                });

                if (item.amigos.length > 0) {
                    html += `</div></div>`;
                }

                html += `
                        </div>
                    </div>
                `;
            });

            content.innerHTML = html;
        } else {
            throw new Error('Erro ao carregar amigos');
        }
    } catch (error) {
        content.innerHTML = `<div class="error-message">Erro ao carregar amigos: ${error.message}</div>`;
    }
}

// Função para carregar compras
async function carregarCompras() {
    const content = document.getElementById('comprasContent');
    content.innerHTML = '<p class="loading">Carregando compras...</p>';

    try {
        const response = await fetch(`${API_BASE_URL}/api/clientes/compras`);
        const data = await response.json();

        if (data.status === 'success') {
            let html = `
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-value">${data.total}</div>
                        <div class="stat-label">Clientes com Compras</div>
                    </div>
                </div>
            `;

            data.clientes_compras.forEach(item => {
                html += `
                    <div class="card">
                        <div class="card-header">
                            🛒 ${item.cliente.nome} (ID: ${item.cliente.id})
                        </div>
                        <div class="card-body">
                            <div class="info-row">
                                <span class="info-label">CPF:</span>
                                <span class="info-value">${item.cliente.cpf}</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Cidade:</span>
                                <span class="info-value">${item.cliente.cidade}</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Total de Compras:</span>
                                <span class="info-value"><span class="badge badge-info">${item.total_compras}</span></span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Valor Total:</span>
                                <span class="info-value"><strong style="color: #28a745;">R$ ${item.valor_total.toFixed(2)}</strong></span>
                            </div>
                            ${item.compras.length > 0 ? `
                                <div class="compras-list">
                                    <strong style="display: block; margin-top: 15px; margin-bottom: 10px;">Histórico de Compras:</strong>
                            ` : '<p style="margin-top: 15px; color: #6c757d;">Nenhuma compra registrada.</p>'}
                `;

                item.compras.forEach(compra => {
                    html += `
                        <div class="compra-item">
                            <div class="compra-produto">${compra.produto}</div>
                            <div class="compra-detalhes">
                                💰 R$ ${compra.valor.toFixed(2)} | 
                                📅 ${new Date(compra.data).toLocaleDateString('pt-BR')} | 
                                🏷️ ${compra.tipo}
                            </div>
                        </div>
                    `;
                });

                if (item.compras.length > 0) {
                    html += `</div>`;
                }

                html += `
                        </div>
                    </div>
                `;
            });

            content.innerHTML = html;
        } else {
            throw new Error('Erro ao carregar compras');
        }
    } catch (error) {
        content.innerHTML = `<div class="error-message">Erro ao carregar compras: ${error.message}</div>`;
    }
}

// Função para carregar recomendações
async function carregarRecomendacoes() {
    const content = document.getElementById('recomendacoesContent');
    content.innerHTML = '<p class="loading">Carregando recomendações...</p>';

    try {
        const response = await fetch(`${API_BASE_URL}/api/recomendacoes`);
        const data = await response.json();

        if (data.status === 'success') {
            let html = `
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-value">${data.total}</div>
                        <div class="stat-label">Clientes com Recomendações</div>
                    </div>
                </div>
            `;

            if (data.recomendacoes.length === 0) {
                html += '<div class="error-message">Nenhuma recomendação disponível. Execute a sincronização primeiro.</div>';
            } else {
                data.recomendacoes.forEach(item => {
                    html += `
                        <div class="card recomendacao-card">
                            <div class="card-header">
                                ⭐ ${item.cliente_nome} (ID: ${item.cliente_id})
                            </div>
                            <div class="card-body">
                                <div class="info-row">
                                    <span class="info-label">CPF:</span>
                                    <span class="info-value">${item.cliente_cpf}</span>
                                </div>
                                <div class="info-row">
                                    <span class="info-label">Total de Recomendações:</span>
                                    <span class="info-value"><span class="badge badge-success">${item.total_recomendacoes}</span></span>
                                </div>
                                <div style="margin-top: 15px;">
                                    <strong>Produtos Recomendados:</strong>
                    `;

                    if (item.recomendacoes.length === 0) {
                        html += '<p style="margin-top: 10px; color: #6c757d;">Nenhuma recomendação disponível para este cliente.</p>';
                    } else {
                        item.recomendacoes.forEach(rec => {
                            const amigosList = rec.amigos_que_compraram.join(', ');
                            html += `
                                <div class="recomendacao-item">
                                    <div class="recomendacao-produto">${rec.produto}</div>
                                    <div class="compra-detalhes">
                                        💰 R$ ${rec.valor.toFixed(2)} | 
                                        🏷️ ${rec.tipo} | 
                                        👥 Recomendado por ${rec.amigos_que_compraram.length} amigo(s)
                                    </div>
                                    <div class="recomendacao-amigos" style="margin-top: 5px;">
                                        Amigos que compraram: ${amigosList}
                                    </div>
                                </div>
                            `;
                        });
                    }

                    html += `
                                </div>
                            </div>
                        </div>
                    `;
                });
            }

            content.innerHTML = html;
        } else {
            throw new Error('Erro ao carregar recomendações');
        }
    } catch (error) {
        content.innerHTML = `<div class="error-message">Erro ao carregar recomendações: ${error.message}</div>`;
    }
}

// Carregar dados da aba ativa ao carregar a página
document.addEventListener('DOMContentLoaded', function() {
    const abaAtiva = document.querySelector('.tab-content.active').id;
    carregarDadosAba(abaAtiva);
});

