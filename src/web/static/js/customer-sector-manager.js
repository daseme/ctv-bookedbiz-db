/**
 * Customer Sector Manager - Main Controller
 * 
 * Orchestrates the Customer Sector Management system following clean architecture principles.
 * Coordinates between API, State, and UI layers with proper separation of concerns.
 */

class CustomerSectorManager {
    constructor() {
        // Initialize core modules
        this.api = new CustomerSectorAPI();
        this.state = new CustomerSectorState();
        this.ui = new CustomerSectorUI(this.state);
        
        // Bind methods to maintain context
        this.initialize = this.initialize.bind(this);
        this.handleCustomerSectorUpdate = this.handleCustomerSectorUpdate.bind(this);
        this.handleBulkUpdate = this.handleBulkUpdate.bind(this);
        this.handleAddNewSector = this.handleAddNewSector.bind(this);
        
        console.log('Customer Sector Manager initialized with clean architecture modules');
    }

    // ============================================================================
    // Initialization
    // ============================================================================

    /**
     * Initialize the application
     */
    async initialize() {
        console.log('Enhanced Customer Sector Manager initializing...');
        
        try {
            // Show loading state
            this.ui.showTableLoading();
            
            // Load initial data from API
            await this.loadInitialData();
            
            // Set up event listeners
            this.setupEventListeners();
            
            // Initial UI render
            this.ui.updateAll();
            
            console.log('Customer Sector Manager ready!');
            
        } catch (error) {
            console.error('Failed to initialize Customer Sector Manager:', error);
            this.ui.showNotification('Failed to initialize application: ' + error.message, 'error');
        }
    }

    /**
     * Load initial data from API
     */
    async loadInitialData() {
        // Load customers and sectors in parallel
        const [customersResponse, sectorsResponse] = await Promise.all([
            this.api.fetchCustomers(),
            this.api.fetchSectors()
        ]);
        
        // Handle customers data
        if (customersResponse.success) {
            this.state.setCustomers(customersResponse.data);
        } else {
            throw new Error(customersResponse.error || 'Failed to load customers');
        }
        
        // Handle sectors data
        if (sectorsResponse.success) {
            this.state.setSectors(sectorsResponse.data);
        } else {
            throw new Error(sectorsResponse.error || 'Failed to load sectors');
        }
        
        // Apply initial filters
        this.state.applyFilters();
    }

    // ============================================================================
    // Event Listeners Setup
    // ============================================================================

    /**
     * Set up all event listeners
     */
    setupEventListeners() {
        this.setupFilterListeners();
        this.setupTableListeners();
        this.setupPaginationListeners();
        this.setupBulkActionListeners();
        this.setupModalListeners();
        this.setupKeyboardListeners();
    }

    /**
     * Set up filter-related event listeners
     */
    setupFilterListeners() {
        const { elements } = this.ui;
        
        // Search input
        if (elements.customerSearch) {
            elements.customerSearch.addEventListener('input', (e) => {
                this.state.updateFilters({ searchQuery: e.target.value });
                this.ui.renderTable();
                this.ui.renderPagination();
                this.ui.updateBulkActionsVisibility();
            });
        }
        
        // Assignment status filter
        if (elements.sectorFilter) {
            elements.sectorFilter.addEventListener('change', (e) => {
                this.state.updateFilters({ sectorFilter: e.target.value });
                this.ui.renderTable();
                this.ui.renderPagination();
                this.ui.updateBulkActionsVisibility();
            });
        }
        
        // Sector-specific filter
        if (elements.sectorSpecificFilter) {
            elements.sectorSpecificFilter.addEventListener('change', (e) => {
                this.state.updateFilters({ sectorSpecificFilter: e.target.value });
                this.ui.renderTable();
                this.ui.renderPagination();
                this.ui.updateBulkActionsVisibility();
            });
        }
        
        // Sort option
        if (elements.sortOption) {
            elements.sortOption.addEventListener('change', (e) => {
                this.state.updateFilters({ sortOption: e.target.value });
                this.ui.renderTable();
                this.ui.renderPagination();
                this.ui.updateBulkActionsVisibility();
            });
        }
        
        // Revenue filter
        if (elements.revenueFilter) {
            elements.revenueFilter.addEventListener('change', (e) => {
                this.state.updateFilters({ revenueFilter: e.target.value });
                this.ui.renderTable();
                this.ui.renderPagination();
                this.ui.updateBulkActionsVisibility();
            });
        }
    }

    /**
     * Set up table-related event listeners using delegation
     */
    setupTableListeners() {
        const { elements } = this.ui;
        
        if (elements.tableContainer) {
            elements.tableContainer.addEventListener('change', (e) => {
                // Handle select all checkbox
                if (e.target.id === 'select-all') {
                    this.handleSelectAllChange(e.target.checked);
                }
                
                // Handle individual customer checkboxes
                if (e.target.classList.contains('customer-checkbox') && e.target.dataset.customerId) {
                    this.handleCustomerSelectionChange(
                        parseInt(e.target.dataset.customerId),
                        e.target.checked
                    );
                }
                
                // Handle sector select changes
                if (e.target.classList.contains('sector-select') && e.target.dataset.customerId) {
                    this.handleCustomerSectorUpdate(
                        parseInt(e.target.dataset.customerId),
                        e.target.value
                    );
                }
            });
        }
    }

    /**
     * Set up pagination event listeners
     */
    setupPaginationListeners() {
        const { elements } = this.ui;
        
        // Previous page button
        if (elements.prevPage) {
            elements.prevPage.addEventListener('click', () => {
                const paginationInfo = this.state.getPaginationInfo();
                if (paginationInfo.hasPrevious) {
                    this.goToPage(this.state.currentPage - 1);
                }
            });
        }
        
        // Next page button
        if (elements.nextPage) {
            elements.nextPage.addEventListener('click', () => {
                const paginationInfo = this.state.getPaginationInfo();
                if (paginationInfo.hasNext) {
                    this.goToPage(this.state.currentPage + 1);
                }
            });
        }
        
        // Page number clicks using delegation
        if (elements.pageNumbers) {
            elements.pageNumbers.addEventListener('click', (e) => {
                if (e.target.classList.contains('page-number') && e.target.dataset.page) {
                    this.goToPage(parseInt(e.target.dataset.page));
                }
            });
        }
    }

    /**
     * Set up bulk action event listeners
     */
    setupBulkActionListeners() {
        const { elements } = this.ui;
        
        // Apply bulk action
        const applyBulk = document.getElementById('apply-bulk');
        if (applyBulk) {
            applyBulk.addEventListener('click', this.handleBulkUpdate);
        }
        
        // Cancel bulk action
        const cancelBulk = document.getElementById('cancel-bulk');
        if (cancelBulk) {
            cancelBulk.addEventListener('click', () => {
                this.state.clearSelections();
                this.ui.renderTable();
                this.ui.updateBulkActionsVisibility();
            });
        }
        
        // Add sector button
        const addSectorBtn = document.getElementById('add-sector-btn');
        if (addSectorBtn) {
            addSectorBtn.addEventListener('click', () => {
                this.ui.showAddSectorModal();
            });
        }
    }

    /**
     * Set up modal event listeners
     */
    setupModalListeners() {
        const { elements } = this.ui;
        
        // Modal close buttons
        const modalCloses = document.querySelectorAll('.modal-close');
        modalCloses.forEach(closeBtn => {
            closeBtn.addEventListener('click', () => {
                this.ui.hideAddSectorModal();
            });
        });
        
        // Add sector form submission
        const addSectorForm = document.querySelector('#add-sector-modal .action-btn.primary');
        if (addSectorForm) {
            addSectorForm.addEventListener('click', this.handleAddNewSector);
        }
        
        // Cancel sector form
        const cancelSectorForm = document.querySelector('#add-sector-modal .action-btn.secondary');
        if (cancelSectorForm) {
            cancelSectorForm.addEventListener('click', () => {
                this.ui.hideAddSectorModal();
            });
        }
    }

    /**
     * Set up keyboard event listeners
     */
    setupKeyboardListeners() {
        document.addEventListener('keydown', (e) => {
            // Escape key closes modals
            if (e.key === 'Escape') {
                this.ui.hideAddSectorModal();
            }
        });
    }

    // ============================================================================
    // Event Handlers
    // ============================================================================

    /**
     * Handle select all checkbox change
     * @param {boolean} checked - New checked state
     */
    handleSelectAllChange(checked) {
        this.state.selectAllOnPage(checked);
        this.ui.renderTable();
        this.ui.updateBulkActionsVisibility();
    }

    /**
     * Handle individual customer selection change
     * @param {number} customerId - Customer ID
     * @param {boolean} checked - New checked state
     */
    handleCustomerSelectionChange(customerId, checked) {
        if (checked) {
            this.state.selectedCustomers.add(customerId);
        } else {
            this.state.selectedCustomers.delete(customerId);
        }
        this.ui.updateBulkActionsVisibility();
    }

    /**
     * Handle customer sector update
     * @param {number} customerId - Customer ID
     * @param {string} newSector - New sector name
     */
    async handleCustomerSectorUpdate(customerId, newSector) {
        // Show loading state
        this.ui.setSectorSelectLoading(customerId, true);
        
        try {
            // Call API
            const response = await this.api.updateCustomerSector(customerId, newSector);
            
            if (response.success) {
                // Update state
                this.state.updateCustomerSector(customerId, newSector);
                
                // Refresh UI
                this.state.applyFilters();
                this.ui.updateStats();
                this.ui.renderTable();
                this.ui.renderPagination();
                this.ui.populateSectorSpecificFilter();
                
                // Show success and highlight row
                this.ui.showNotification(response.message, 'success');
                this.ui.highlightCustomerRow(customerId);
                
            } else {
                throw new Error(response.error);
            }
            
        } catch (error) {
            console.error('Error updating customer sector:', error);
            this.ui.showNotification('Failed to update sector: ' + error.message, 'error');
            
            // Revert select element
            const selectElement = document.getElementById(`sector-select-${customerId}`);
            const customer = this.state.customers.find(c => c.id === customerId);
            if (selectElement && customer) {
                selectElement.value = customer.sector || '';
            }
        } finally {
            // Remove loading state
            this.ui.setSectorSelectLoading(customerId, false);
        }
    }

    /**
     * Handle bulk sector update
     */
    async handleBulkUpdate() {
        const { elements } = this.ui;
        const selectedSector = elements.bulkSectorSelect ? elements.bulkSectorSelect.value : '';
        const selectedIds = Array.from(this.state.selectedCustomers);
        
        if (!selectedSector) {
            this.ui.showNotification('Please select a sector to assign', 'error');
            return;
        }
        
        if (selectedIds.length === 0) {
            this.ui.showNotification('No customers selected', 'error');
            return;
        }
        
        try {
            // Call API
            const response = await this.api.bulkUpdateCustomerSectors(selectedIds, selectedSector);
            
            if (response.success) {
                // Update state
                this.state.bulkUpdateCustomerSectors(selectedIds, selectedSector);
                this.state.clearSelections();
                
                // Refresh UI
                this.state.applyFilters();
                this.ui.updateAll();
                
                // Show success
                this.ui.showNotification(response.message, 'success');
                
            } else {
                throw new Error(response.error);
            }
            
        } catch (error) {
            console.error('Error in bulk update:', error);
            this.ui.showNotification('Bulk update failed: ' + error.message, 'error');
        }
    }

    /**
     * Handle adding new sector
     */
    async handleAddNewSector() {
        const formData = this.ui.getNewSectorFormData();
        
        if (!formData.name) {
            this.ui.showNotification('Sector name is required', 'error');
            return;
        }
        
        try {
            // Call API
            const response = await this.api.createSector(formData.name, formData.group);
            
            if (response.success) {
                // Update state
                this.state.addSector(response.data);
                
                // Refresh UI
                this.ui.updateStats();
                this.ui.populateBulkSectorSelect();
                this.ui.populateSectorSpecificFilter();
                this.ui.hideAddSectorModal();
                
                // Show success
                this.ui.showNotification(response.message, 'success');
                
            } else {
                throw new Error(response.error);
            }
            
        } catch (error) {
            console.error('Error adding sector:', error);
            this.ui.showNotification('Failed to add sector: ' + error.message, 'error');
        }
    }

    /**
     * Handle page navigation
     * @param {number} page - Page number to navigate to
     */
    goToPage(page) {
        if (this.state.setCurrentPage(page)) {
            this.ui.renderTable();
            this.ui.renderPagination();
        }
    }

    // ============================================================================
    // Public API Methods
    // ============================================================================

    /**
     * Clear all filters (exposed for template button)
     */
    clearFilters() {
        this.state.clearAllFilters();
        this.ui.clearFormElements();
        this.ui.renderTable();
        this.ui.renderPagination();
        this.ui.updateBulkActionsVisibility();
        this.ui.showNotification('Filters cleared', 'success');
    }

    /**
     * Export data (placeholder for future implementation)
     */
    exportData() {
        this.ui.showNotification('Export functionality coming soon!', 'success');
    }

    /**
     * Refresh all data from server
     */
    async refreshData() {
        try {
            this.ui.showTableLoading();
            await this.loadInitialData();
            this.ui.updateAll();
            this.ui.showNotification('Data refreshed successfully', 'success');
        } catch (error) {
            console.error('Error refreshing data:', error);
            this.ui.showNotification('Failed to refresh data: ' + error.message, 'error');
        }
    }

    /**
     * Get current application state (for debugging)
     * @returns {Object} Current state summary
     */
    getStateSummary() {
        return this.state.getStateSummary();
    }

    /**
     * Check system health
     * @returns {Promise<boolean>} System health status
     */
    async checkHealth() {
        return await this.api.checkHealth();
    }
}

// ============================================================================
// Global Functions (for template compatibility)
// ============================================================================

// These functions maintain compatibility with the original template
// while delegating to the clean architecture

let customerSectorManager;

/**
 * Clear filters (template compatibility)
 */
function clearFilters() {
    if (customerSectorManager) {
        customerSectorManager.clearFilters();
    }
}

/**
 * Export data (template compatibility)
 */
function exportData() {
    if (customerSectorManager) {
        customerSectorManager.exportData();
    }
}

/**
 * Close add sector modal (template compatibility)
 */
function closeAddSectorModal() {
    if (customerSectorManager) {
        customerSectorManager.ui.hideAddSectorModal();
    }
}

/**
 * Add new sector (template compatibility)
 */
function addNewSector() {
    if (customerSectorManager) {
        customerSectorManager.handleAddNewSector();
    }
}

// ============================================================================
// Application Bootstrap
// ============================================================================

/**
 * Initialize the application when DOM is ready
 */
document.addEventListener('DOMContentLoaded', async function() {
    try {
        // Create manager instance
        customerSectorManager = new CustomerSectorManager();
        
        // Make it globally available for debugging and template compatibility
        window.customerSectorManager = customerSectorManager;
        
        // Initialize the application
        await customerSectorManager.initialize();
        
        // Force populate sector filter after page loads (compatibility)
        setTimeout(() => {
            if (customerSectorManager.state.customers.length > 0) {
                customerSectorManager.ui.populateSectorSpecificFilter();
                const uniqueSectors = customerSectorManager.state.getUniqueSectors();
                console.log('Sector filter populated with', uniqueSectors.length, 'sectors');
            }
        }, 200);
        
    } catch (error) {
        console.error('Failed to initialize Customer Sector Manager:', error);
        
        // Show error in UI if possible
        const notification = document.getElementById('notification');
        if (notification) {
            notification.textContent = 'Application failed to initialize: ' + error.message;
            notification.className = 'notification error show';
        }
    }
});

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CustomerSectorManager;
} else {
    // Browser global
    window.CustomerSectorManager = CustomerSectorManager;
}