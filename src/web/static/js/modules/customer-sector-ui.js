/**
 * Customer Sector UI Management
 * 
 * Handles all DOM manipulation, rendering, and UI interactions.
 * Provides a clean interface for updating the UI based on state changes.
 */

class CustomerSectorUI {
    constructor(state) {
        this.state = state;
        
        // Cache DOM elements for performance
        this.elements = {
            // Stats elements
            totalCustomers: document.getElementById('total-customers'),
            assignedCustomers: document.getElementById('assigned-customers'),
            unassignedCustomers: document.getElementById('unassigned-customers'),
            totalSectors: document.getElementById('total-sectors'),
            
            // Filter elements
            customerSearch: document.getElementById('customer-search'),
            sectorFilter: document.getElementById('sector-filter'),
            sectorSpecificFilter: document.getElementById('sector-specific-filter'),
            sortOption: document.getElementById('sort-option'),
            revenueFilter: document.getElementById('revenue-filter'),
            
            // Table and pagination
            tableContainer: document.getElementById('table-container'),
            paginationContainer: document.getElementById('pagination-container'),
            paginationInfo: document.getElementById('pagination-info'),
            pageNumbers: document.getElementById('page-numbers'),
            prevPage: document.getElementById('prev-page'),
            nextPage: document.getElementById('next-page'),
            
            // Bulk operations
            bulkOperations: document.getElementById('bulk-operations'),
            bulkInfo: document.getElementById('bulk-info'),
            bulkSectorSelect: document.getElementById('bulk-sector-select'),
            
            // Modal
            addSectorModal: document.getElementById('add-sector-modal'),
            newSectorName: document.getElementById('new-sector-name'),
            newSectorGroup: document.getElementById('new-sector-group'),
            
            // Notification
            notification: document.getElementById('notification')
        };
        
        // Validate required elements
        this.validateElements();
    }

    // ============================================================================
    // Element Validation
    // ============================================================================

    /**
     * Validate that required DOM elements exist
     */
    validateElements() {
        const missing = [];
        
        Object.entries(this.elements).forEach(([key, element]) => {
            if (!element) {
                missing.push(key);
            }
        });
        
        if (missing.length > 0) {
            console.warn('Missing DOM elements:', missing);
        }
    }

    // ============================================================================
    // Statistics Display
    // ============================================================================

    /**
     * Update statistics display
     */
    updateStats() {
        const stats = this.state.getCustomerStats();
        
        if (this.elements.totalCustomers) {
            this.elements.totalCustomers.textContent = stats.total;
        }
        if (this.elements.assignedCustomers) {
            this.elements.assignedCustomers.textContent = stats.assigned;
        }
        if (this.elements.unassignedCustomers) {
            this.elements.unassignedCustomers.textContent = stats.unassigned;
        }
        if (this.elements.totalSectors) {
            this.elements.totalSectors.textContent = stats.totalSectors;
        }
    }

    // ============================================================================
    // Table Rendering
    // ============================================================================

    /**
     * Render the customer table
     */
    renderTable() {
        if (!this.elements.tableContainer) return;
        
        const currentCustomers = this.state.getCurrentPageCustomers();
        
        if (currentCustomers.length === 0) {
            this.renderEmptyState();
            return;
        }
        
        const selectionState = this.state.getPageSelectionState();
        
        let tableHtml = `
            <table class="customer-table">
                <thead>
                    <tr>
                        <th class="checkbox-column">
                            <input type="checkbox" 
                                   id="select-all" 
                                   class="customer-checkbox"
                                   ${selectionState.allSelected ? 'checked' : ''}>
                        </th>
                        <th>Customer Name</th>
                        <th>Current Sector</th>
                        <th>Assign Sector</th>
                        <th>Status</th>
                        <th>Revenue</th>
                        <th>Last Updated</th>
                    </tr>
                </thead>
                <tbody>
        `;
        
        currentCustomers.forEach(customer => {
            const isSelected = this.state.selectedCustomers.has(customer.id);
            const sectorOptions = this.generateSectorOptions(customer.sector);
            const revenueFormatted = this.formatRevenue(customer.totalRevenue);
            
            tableHtml += `
                <tr class="customer-row ${isSelected ? 'selected' : ''}" 
                    id="customer-row-${customer.id}">
                    <td class="checkbox-column">
                        <input type="checkbox" 
                               class="customer-checkbox" 
                               data-customer-id="${customer.id}"
                               ${isSelected ? 'checked' : ''}>
                    </td>
                    <td>
                        <div class="customer-name" title="${customer.name}">${customer.name}</div>
                    </td>
                    <td>
                        <span class="sector-badge ${customer.sector ? 'assigned' : 'unassigned'}">
                            ${customer.sector || 'Unassigned'}
                        </span>
                    </td>
                    <td>
                        <select id="sector-select-${customer.id}" 
                                class="sector-select" 
                                data-customer-id="${customer.id}">
                            ${sectorOptions}
                        </select>
                    </td>
                    <td>
                        <div class="status-indicator">
                            <div class="status-dot ${customer.sector ? 'assigned' : 'unassigned'}"></div>
                            ${customer.sector ? 'Assigned' : 'Needs Assignment'}
                        </div>
                    </td>
                    <td style="color: var(--nord3); font-size: 14px; font-weight: 600;">
                        ${revenueFormatted}
                    </td>
                    <td style="color: var(--nord3); font-size: 13px;">
                        ${customer.lastUpdated}
                    </td>
                </tr>
            `;
        });
        
        tableHtml += '</tbody></table>';
        this.elements.tableContainer.innerHTML = tableHtml;
        
        // Update select all checkbox state
        const selectAllCheckbox = document.getElementById('select-all');
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = selectionState.allSelected;
            selectAllCheckbox.indeterminate = selectionState.someSelected;
        }
    }

    /**
     * Render empty state when no customers match filters
     */
    renderEmptyState() {
        this.elements.tableContainer.innerHTML = `
            <div class="loading-state">
                <div>No customers found matching current filters.</div>
                <button onclick="window.customerSectorManager.clearFilters()" 
                        class="action-btn secondary" 
                        style="margin-top: 15px;">
                    Clear Filters
                </button>
            </div>
        `;
    }

    /**
     * Generate sector options HTML for select elements
     * @param {string|null} currentSector - Currently selected sector
     * @returns {string} HTML options string
     */
    generateSectorOptions(currentSector) {
        let options = '<option value="">Unassigned</option>';
        
        this.state.sectors.forEach(sector => {
            const selected = sector.name === currentSector ? 'selected' : '';
            options += `<option value="${sector.name}" ${selected}>${sector.name}</option>`;
        });
        
        return options;
    }

    /**
     * Format revenue for display
     * @param {number} revenue - Revenue amount
     * @returns {string} Formatted revenue string
     */
    formatRevenue(revenue) {
        if (!revenue || revenue === 0) return '$0';
        return `$${revenue.toLocaleString('en-US', {maximumFractionDigits: 0})}`;
    }

    /**
     * Highlight a customer row briefly (for updates)
     * @param {number} customerId - Customer ID
     */
    highlightCustomerRow(customerId) {
        const row = document.getElementById(`customer-row-${customerId}`);
        if (row) {
            row.classList.add('recently-changed');
            setTimeout(() => row.classList.remove('recently-changed'), 2000);
        }
    }

    // ============================================================================
    // Pagination Rendering
    // ============================================================================

    /**
     * Render pagination controls
     */
    renderPagination() {
        const paginationInfo = this.state.getPaginationInfo();
        
        if (paginationInfo.totalPages <= 1) {
            this.elements.paginationContainer.style.display = 'none';
            return;
        }
        
        this.elements.paginationContainer.style.display = 'flex';
        
        // Update info text
        if (this.elements.paginationInfo) {
            this.elements.paginationInfo.textContent = 
                `Showing ${paginationInfo.startIndex}-${paginationInfo.endIndex} of ${paginationInfo.totalCustomers} customers`;
        }
        
        // Generate page numbers
        const pageNumbers = this.generatePageNumbers(paginationInfo.currentPage, paginationInfo.totalPages);
        const pageNumbersHtml = pageNumbers.map(page => 
            page === '...' 
                ? '<span style="padding: 8px;">...</span>'
                : `<span class="page-number ${page === paginationInfo.currentPage ? 'active' : ''}" 
                         data-page="${page}">${page}</span>`
        ).join('');
        
        if (this.elements.pageNumbers) {
            this.elements.pageNumbers.innerHTML = pageNumbersHtml;
        }
        
        // Update navigation buttons
        if (this.elements.prevPage) {
            this.elements.prevPage.disabled = !paginationInfo.hasPrevious;
        }
        if (this.elements.nextPage) {
            this.elements.nextPage.disabled = !paginationInfo.hasNext;
        }
    }

    /**
     * Generate page numbers for pagination
     * @param {number} current - Current page
     * @param {number} total - Total pages
     * @returns {Array} Array of page numbers and ellipsis
     */
    generatePageNumbers(current, total) {
        if (total <= 7) {
            return Array.from({length: total}, (_, i) => i + 1);
        }
        
        if (current <= 4) {
            return [1, 2, 3, 4, 5, '...', total];
        }
        
        if (current >= total - 3) {
            return [1, '...', total - 4, total - 3, total - 2, total - 1, total];
        }
        
        return [1, '...', current - 1, current, current + 1, '...', total];
    }

    // ============================================================================
    // Filter UI Updates
    // ============================================================================

    /**
     * Populate sector-specific filter dropdown
     */
    populateSectorSpecificFilter() {
        if (!this.elements.sectorSpecificFilter) return;
        
        let options = '<option value="">All Sectors</option>';
        const uniqueSectors = this.state.getUniqueSectors();
        
        uniqueSectors.forEach(sectorName => {
            options += `<option value="${sectorName}">${sectorName}</option>`;
        });
        
        this.elements.sectorSpecificFilter.innerHTML = options;
    }

    /**
     * Populate bulk sector select dropdown
     */
    populateBulkSectorSelect() {
        if (!this.elements.bulkSectorSelect) return;
        
        let options = '<option value="">Assign selected to...</option>';
        
        this.state.sectors.forEach(sector => {
            options += `<option value="${sector.name}">${sector.name}</option>`;
        });
        
        this.elements.bulkSectorSelect.innerHTML = options;
    }

    /**
     * Update bulk actions visibility and info
     */
    updateBulkActionsVisibility() {
        if (!this.elements.bulkOperations || !this.elements.bulkInfo) return;
        
        const selectedCount = this.state.selectedCustomers.size;
        const hasSelection = selectedCount > 0;
        
        this.elements.bulkOperations.style.display = hasSelection ? 'flex' : 'none';
        
        if (hasSelection) {
            this.elements.bulkInfo.textContent = 
                `📋 ${selectedCount} customer${selectedCount > 1 ? 's' : ''} selected`;
        }
    }

    // ============================================================================
    // Loading States
    // ============================================================================

    /**
     * Show loading state in table
     */
    showTableLoading() {
        if (this.elements.tableContainer) {
            this.elements.tableContainer.innerHTML = `
                <div class="loading-state">
                    <div class="loading-spinner"></div>
                    <div>Loading customer data...</div>
                </div>
            `;
        }
    }

    /**
     * Set loading state for sector select
     * @param {number} customerId - Customer ID
     * @param {boolean} loading - Loading state
     */
    setSectorSelectLoading(customerId, loading) {
        const selectElement = document.getElementById(`sector-select-${customerId}`);
        if (selectElement) {
            selectElement.classList.toggle('loading', loading);
            selectElement.disabled = loading;
        }
    }

    // ============================================================================
    // Modal Management
    // ============================================================================

    /**
     * Show add sector modal
     */
    showAddSectorModal() {
        if (this.elements.addSectorModal) {
            this.elements.addSectorModal.style.display = 'flex';
            if (this.elements.newSectorName) {
                this.elements.newSectorName.focus();
            }
        }
    }

    /**
     * Hide add sector modal and clear form
     */
    hideAddSectorModal() {
        if (this.elements.addSectorModal) {
            this.elements.addSectorModal.style.display = 'none';
        }
        if (this.elements.newSectorName) {
            this.elements.newSectorName.value = '';
        }
        if (this.elements.newSectorGroup) {
            this.elements.newSectorGroup.value = '';
        }
    }

    /**
     * Get new sector form data
     * @returns {Object} Form data object
     */
    getNewSectorFormData() {
        return {
            name: this.elements.newSectorName ? this.elements.newSectorName.value.trim() : '',
            group: this.elements.newSectorGroup ? this.elements.newSectorGroup.value.trim() : ''
        };
    }

    // ============================================================================
    // Notification System
    // ============================================================================

    /**
     * Show notification message
     * @param {string} message - Notification message
     * @param {string} type - Notification type ('success' or 'error')
     */
    showNotification(message, type = 'success') {
        if (!this.elements.notification) return;
        
        this.elements.notification.textContent = message;
        this.elements.notification.className = `notification ${type}`;
        this.elements.notification.classList.add('show');
        
        // Auto-hide after 4 seconds
        setTimeout(() => {
            this.elements.notification.classList.remove('show');
        }, 4000);
    }

    // ============================================================================
    // Form State Management
    // ============================================================================

    /**
     * Update form elements with current state
     */
    updateFormElements() {
        if (this.elements.customerSearch) {
            this.elements.customerSearch.value = this.state.searchQuery;
        }
        if (this.elements.sectorFilter) {
            this.elements.sectorFilter.value = this.state.sectorFilter;
        }
        if (this.elements.sectorSpecificFilter) {
            this.elements.sectorSpecificFilter.value = this.state.sectorSpecificFilter;
        }
        if (this.elements.sortOption) {
            this.elements.sortOption.value = this.state.sortOption;
        }
        if (this.elements.revenueFilter) {
            this.elements.revenueFilter.value = this.state.revenueFilter;
        }
    }

    /**
     * Clear all form elements
     */
    clearFormElements() {
        if (this.elements.customerSearch) {
            this.elements.customerSearch.value = '';
        }
        if (this.elements.sectorFilter) {
            this.elements.sectorFilter.value = 'all';
        }
        if (this.elements.sectorSpecificFilter) {
            this.elements.sectorSpecificFilter.value = '';
        }
        if (this.elements.sortOption) {
            this.elements.sortOption.value = 'name';
        }
        if (this.elements.revenueFilter) {
            this.elements.revenueFilter.value = '';
        }
    }

    // ============================================================================
    // Full UI Update
    // ============================================================================

    /**
     * Update entire UI based on current state
     */
    updateAll() {
        this.updateStats();
        this.renderTable();
        this.renderPagination();
        this.updateBulkActionsVisibility();
        this.populateSectorSpecificFilter();
        this.populateBulkSectorSelect();
    }

    /**
     * Refresh UI after data changes
     */
    refresh() {
        this.updateAll();
        console.log('UI refreshed');
    }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CustomerSectorUI;
} else {
    // Browser global
    window.CustomerSectorUI = CustomerSectorUI;
}