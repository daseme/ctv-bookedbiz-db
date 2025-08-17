/**
 * Decay Timeline and Customer Modal Components
 */

const DecayTimeline = {
    /**
     * Show decay timeline modal for a specific month
     */
    async show(month) {
        if (!window.PipelineController.getCurrentAE()) {
            PipelineUI.showAlert('error', 'No AE selected');
            return;
        }
        
        try {
            const currentAE = window.PipelineController.getCurrentAE();
            const result = await PipelineAPI.loadDecayTimeline(currentAE.ae_id, month);
            
            if (result.success) {
                const modal = document.getElementById('decay-timeline-modal');
                const title = document.getElementById('timeline-title');
                const body = document.getElementById('timeline-body');
                
                title.textContent = `Decay Timeline - ${currentAE.name} - ${PipelineUtils.formatMonthYear(month)}`;
                
                // Populate timeline events
                const timeline = result.data.timeline || [];
                body.innerHTML = '';
                
                if (timeline.length === 0) {
                    body.innerHTML = '<p style="text-align: center; color: var(--nord2);">No decay events for this month.</p>';
                } else {
                    timeline.forEach(event => {
                        const eventElement = this.createTimelineEvent(event);
                        body.appendChild(eventElement);
                    });
                }
                
                modal.style.display = 'block';
            } else {
                PipelineUI.showAlert('error', 'Failed to load decay timeline');
            }
        } catch (error) {
            console.error('Error loading decay timeline:', error);
            PipelineUI.showAlert('error', 'Error loading decay timeline');
        }
    },
    
    /**
     * Create timeline event element
     */
    createTimelineEvent(event) {
        const eventDiv = document.createElement('div');
        eventDiv.className = `timeline-event ${event.event_type}`;
        
        const iconText = PIPELINE_CONFIG.decay.eventIcons[event.event_type] || '•';
        const eventDate = PipelineUtils.formatDate(event.timestamp);
        
        eventDiv.innerHTML = `
            <div class="timeline-icon ${event.event_type}">
                ${iconText}
            </div>
            <div class="timeline-details">
                <div class="timeline-title">${event.description || event.event_type}</div>
                <div class="timeline-description">
                    ${event.customer ? `Customer: ${event.customer} • ` : ''}
                    Amount: ${PipelineUtils.formatCurrency(event.amount)} • 
                    Pipeline: ${PipelineUtils.formatCurrency(event.old_pipeline)} → ${PipelineUtils.formatCurrency(event.new_pipeline)}
                </div>
                <div class="timeline-meta">
                    <span>📅 ${eventDate}</span>
                    <span>👤 ${event.created_by}</span>
                </div>
            </div>
        `;
        
        return eventDiv;
    },
    
    /**
     * Close decay timeline modal
     */
    close() {
        const modal = document.getElementById('decay-timeline-modal');
        if (modal) {
            modal.style.display = 'none';
        }
    }
};

const CustomerModal = {
    currentData: null,
    
    /**
     * Open customer modal for a specific month
     */
    async open(month) {
        if (!window.PipelineController.getCurrentAE()) {
            PipelineUI.showAlert('error', 'No AE selected');
            return;
        }
        
        try {
            const currentAE = window.PipelineController.getCurrentAE();
            const result = await PipelineAPI.loadCustomerData(currentAE.ae_id, month);
            
            if (result.success) {
                this.currentData = result.data;
                const totalCustomers = this.currentData.all_deals ? this.currentData.all_deals.length : 0;
                
                document.getElementById('modal-title').textContent = 
                    `${currentAE.name} - ${PipelineUtils.formatMonthYear(month)} (${totalCustomers} customers)`;
                
                this.populateTable('booked-deals', this.currentData.booked_deals || []);
                this.populateTable('pipeline-deals', this.currentData.pipeline_deals || []);
                this.populateTable('all-deals', this.currentData.all_deals || []);
                
                document.getElementById('customer-modal').style.display = 'block';
            } else {
                PipelineUI.showAlert('error', result.error || 'Failed to load customer data');
            }
        } catch (error) {
            console.error('Error loading customer data:', error);
            PipelineUI.showAlert('error', 'Failed to load customer data');
        }
    },
    
    /**
     * Populate customer table
     */
    populateTable(tableId, customers) {
        const tbody = document.getElementById(tableId);
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        if (customers.length === 0) {
            const message = tableId === 'pipeline-deals' ? 
                'No pipeline deals from historical data' : 
                'No customers found';
            tbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: var(--nord2);">${message}</td></tr>`;
            return;
        }
        
        customers.forEach(customer => {
            const row = document.createElement('tr');
            
            let status = 'Booked';
            let statusClass = 'status-booked';
            if (tableId === 'pipeline-deals') {
                status = 'Pipeline';
                statusClass = 'status-pipeline';
            }
            
            // Handle different table structures
            if (tableId === 'pipeline-deals') {
                row.innerHTML = `
                    <td>${customer.customer_name || 'Unknown Customer'}</td>
                    <td>${customer.deal_description || 'N/A'}</td>
                    <td class="deal-amount">${PipelineUtils.formatCurrency(customer.amount || 0)}</td>
                    <td>${customer.probability || 'N/A'}%</td>
                    <td>${customer.expected_close || 'N/A'}</td>
                    <td><span class="deal-status ${statusClass}">${status}</span></td>
                `;
            } else {
                row.innerHTML = `
                    <td>${customer.customer_name || 'Unknown Customer'}</td>
                    <td>${customer.spot_count || 0} spots</td>
                    <td class="deal-amount">${PipelineUtils.formatCurrency(customer.total_revenue || 0)}</td>
                    <td>${customer.first_spot ? customer.first_spot.split(' ')[0] : 'N/A'}</td>
                    <td><span class="deal-status ${statusClass}">${status}</span></td>
                `;
            }
            
            tbody.appendChild(row);
        });
    },
    
    /**
     * Switch between tabs
     */
    switchTab(tabName) {
        // Update tab buttons
        document.querySelectorAll('.tab').forEach(tab => {
            tab.classList.remove('active');
        });
        event.target.classList.add('active');
        
        // Update tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
        });
        const targetContent = document.getElementById(`${tabName}-content`);
        if (targetContent) {
            targetContent.classList.add('active');
        }
    },
    
    /**
     * Filter customer data based on search term
     */
    filterData: PipelineUtils.debounce(function(searchTerm) {
        const term = searchTerm.toLowerCase();
        document.querySelectorAll('.deals-table tbody tr').forEach(row => {
            const text = row.textContent.toLowerCase();
            row.style.display = text.includes(term) ? '' : 'none';
        });
    }, 300),
    
    /**
     * Close customer modal
     */
    close() {
        const modal = document.getElementById('customer-modal');
        if (modal) {
            modal.style.display = 'none';
        }
        
        const searchBox = document.getElementById('customer-search');
        if (searchBox) {
            searchBox.value = '';
        }
        
        this.currentData = null;
    }
};

// Export for use in other modules
window.DecayTimeline = DecayTimeline;
window.CustomerModal = CustomerModal;