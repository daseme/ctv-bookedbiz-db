/**
 * Customer Sector State Management
 * 
 * Handles all application state, filtering, and sorting logic.
 * Provides a clean interface for state mutations with proper encapsulation.
 */

class CustomerSectorState {
    constructor() {
        // Core data collections
        this.customers = [];
        this.sectors = [];
        this.filteredCustomers = [];
        
        // Selection state
        this.selectedCustomers = new Set();
        
        // Pagination state
        this.currentPage = 1;
        this.customersPerPage = 50;
        
        // Filter state
        this.searchQuery = '';
        this.sectorFilter = 'all'; // 'all', 'assigned', 'unassigned'
        this.sectorSpecificFilter = '';
        this.sortOption = 'name';
        this.revenueFilter = '';
        
        // UI state
        this.isLoading = false;
        this.lastUpdated = null;
        
        // Bind methods to ensure proper context
        this.applyFilters = this.applyFilters.bind(this);
        this.applySorting = this.applySorting.bind(this);
    }

    // ============================================================================
    // Data Management
    // ============================================================================

    /**
     * Set customers data with validation
     * @param {Array} customers - Array of customer objects
     */
    setCustomers(customers) {
        if (!Array.isArray(customers)) {
            throw new Error('Customers must be an array');
        }
        
        this.customers = customers.map(customer => ({
            ...customer,
            // Ensure required fields have defaults
            id: customer.id,
            name: customer.name || 'Unknown Customer',
            sector: customer.sector || null,
            totalRevenue: customer.totalRevenue || 0,
            lastUpdated: customer.lastUpdated || new Date().toISOString().split('T')[0]
        }));
        
        this.filteredCustomers = [...this.customers];
        this.lastUpdated = new Date();
        
        console.log(`State updated with ${this.customers.length} customers`);
    }

    /**
     * Set sectors data with validation
     * @param {Array} sectors - Array of sector objects
     */
    setSectors(sectors) {
        if (!Array.isArray(sectors)) {
            throw new Error('Sectors must be an array');
        }
        
        this.sectors = sectors.map(sector => ({
            ...sector,
            id: sector.id,
            name: sector.name || 'Unknown Sector',
            description: sector.description || null,
            color: sector.color || '#88c0d0'
        }));
        
        console.log(`State updated with ${this.sectors.length} sectors`);
    }

    /**
     * Update a single customer's sector
     * @param {number} customerId - Customer ID
     * @param {string|null} newSector - New sector name
     */
    updateCustomerSector(customerId, newSector) {
        const customer = this.customers.find(c => c.id === customerId);
        if (customer) {
            customer.sector = newSector;
            customer.lastUpdated = new Date().toISOString().split('T')[0];
            console.log(`Updated customer ${customerId} sector to: ${newSector || 'Unassigned'}`);
            return true;
        }
        return false;
    }

    /**
     * Add a new sector
     * @param {Object} sectorData - New sector object
     */
    addSector(sectorData) {
        if (!sectorData.name) {
            throw new Error('Sector name is required');
        }
        
        this.sectors.push({
            id: sectorData.id,
            name: sectorData.name,
            description: sectorData.description || null,
            color: sectorData.color || '#88c0d0'
        });
        
        console.log(`Added new sector: ${sectorData.name}`);
    }

    /**
     * Update customer ID for an unresolved customer after it's been created
     * @param {string} customerName - Customer name to find
     * @param {number} newCustomerId - New customer ID from database
     */
    updateCustomerId(customerName, newCustomerId) {
        const customer = this.customers.find(c => c.name === customerName && c.id === 0);
        if (customer) {
            customer.id = newCustomerId;
            customer.isUnresolved = false;  // No longer unresolved
            console.log(`Updated customer "${customerName}" from ID 0 to ID ${newCustomerId}`);
            
            // Update filtered customers as well
            const filteredCustomer = this.filteredCustomers.find(c => c.name === customerName && c.id === newCustomerId);
            if (filteredCustomer) {
                filteredCustomer.id = newCustomerId;
                filteredCustomer.isUnresolved = false;
            }
            
            return true;
        }
        return false;
    }

    /**
     * Bulk update customer sectors
     * @param {number[]} customerIds - Array of customer IDs
     * @param {string} sectorName - Sector name to assign
     */
    bulkUpdateCustomerSectors(customerIds, sectorName) {
        const today = new Date().toISOString().split('T')[0];
        let updateCount = 0;
        
        customerIds.forEach(id => {
            const customer = this.customers.find(c => c.id === id);
            if (customer) {
                customer.sector = sectorName;
                customer.lastUpdated = today;
                updateCount++;
            }
        });
        
        console.log(`Bulk updated ${updateCount} customers to sector: ${sectorName}`);
        return updateCount;
    }

    // ============================================================================
    // Selection Management
    // ============================================================================

    /**
     * Toggle customer selection
     * @param {number} customerId - Customer ID
     * @returns {boolean} True if now selected, false if deselected
     */
    toggleCustomerSelection(customerId) {
        if (this.selectedCustomers.has(customerId)) {
            this.selectedCustomers.delete(customerId);
            return false;
        } else {
            this.selectedCustomers.add(customerId);
            return true;
        }
    }

    /**
     * Select all customers on current page
     * @param {boolean} selected - True to select all, false to deselect all
     */
    selectAllOnPage(selected) {
        const startIndex = (this.currentPage - 1) * this.customersPerPage;
        const endIndex = startIndex + this.customersPerPage;
        const currentCustomers = this.filteredCustomers.slice(startIndex, endIndex);
        
        if (selected) {
            currentCustomers.forEach(customer => this.selectedCustomers.add(customer.id));
        } else {
            currentCustomers.forEach(customer => this.selectedCustomers.delete(customer.id));
        }
    }

    /**
     * Clear all selections
     */
    clearSelections() {
        this.selectedCustomers.clear();
    }

    /**
     * Get selection state for current page
     * @returns {Object} Selection information
     */
    getPageSelectionState() {
        const startIndex = (this.currentPage - 1) * this.customersPerPage;
        const endIndex = startIndex + this.customersPerPage;
        const currentCustomers = this.filteredCustomers.slice(startIndex, endIndex);
        const selectedOnPage = currentCustomers.filter(c => this.selectedCustomers.has(c.id)).length;
        
        return {
            total: currentCustomers.length,
            selected: selectedOnPage,
            allSelected: selectedOnPage === currentCustomers.length && currentCustomers.length > 0,
            someSelected: selectedOnPage > 0 && selectedOnPage < currentCustomers.length
        };
    }

    // ============================================================================
    // Filter & Sort Logic
    // ============================================================================

    /**
     * Apply all active filters and sorting
     */
    applyFilters() {
        let filtered = [...this.customers];
        
        // Apply search filter
        if (this.searchQuery.trim()) {
            const query = this.searchQuery.toLowerCase().trim();
            filtered = filtered.filter(customer => 
                customer.name.toLowerCase().includes(query)
            );
        }
        
        // Apply assignment status filter
        if (this.sectorFilter === 'assigned') {
            filtered = filtered.filter(customer => customer.sector);
        } else if (this.sectorFilter === 'unassigned') {
            filtered = filtered.filter(customer => !customer.sector);
        }
        
        // Apply sector-specific filter
        if (this.sectorSpecificFilter) {
            filtered = filtered.filter(customer => customer.sector === this.sectorSpecificFilter);
        }
        
        // Apply revenue filter
        if (this.revenueFilter) {
            filtered = filtered.filter(customer => {
                const revenue = customer.totalRevenue || 0;
                switch (this.revenueFilter) {
                    case 'high': return revenue >= 100000;
                    case 'medium': return revenue >= 25000 && revenue < 100000;
                    case 'low': return revenue > 0 && revenue < 25000;
                    case 'zero': return revenue === 0;
                    default: return true;
                }
            });
        }
        
        // Apply sorting
        filtered = this.applySorting(filtered);
        
        this.filteredCustomers = filtered;
        this.currentPage = 1; // Reset to first page
        this.selectedCustomers.clear(); // Clear selections
        
        console.log(`Applied filters: ${this.customers.length} → ${filtered.length} customers`);
    }

    /**
     * Apply sorting to customers array
     * @param {Array} customers - Array of customers to sort
     * @returns {Array} Sorted array
     */
    applySorting(customers) {
        const sorted = [...customers];
        
        switch (this.sortOption) {
            case 'name':
                return sorted.sort((a, b) => a.name.localeCompare(b.name));
            case 'name_desc':
                return sorted.sort((a, b) => b.name.localeCompare(a.name));
            case 'status':
                return sorted.sort((a, b) => {
                    if (!a.sector && b.sector) return -1;
                    if (a.sector && !b.sector) return 1;
                    return a.name.localeCompare(b.name);
                });
            case 'revenue_desc':
                return sorted.sort((a, b) => (b.totalRevenue || 0) - (a.totalRevenue || 0));
            case 'revenue_asc':
                return sorted.sort((a, b) => (a.totalRevenue || 0) - (b.totalRevenue || 0));
            case 'updated_desc':
                return sorted.sort((a, b) => new Date(b.lastUpdated) - new Date(a.lastUpdated));
            case 'sector':
                return sorted.sort((a, b) => {
                    const aSector = a.sector || 'ZZ_Unassigned';
                    const bSector = b.sector || 'ZZ_Unassigned';
                    return aSector.localeCompare(bSector);
                });
            default:
                return sorted;
        }
    }

    /**
     * Update filter state and apply filters
     * @param {Object} filterUpdates - Object with filter properties to update
     */
    updateFilters(filterUpdates) {
        Object.keys(filterUpdates).forEach(key => {
            if (this.hasOwnProperty(key)) {
                this[key] = filterUpdates[key];
            }
        });
        
        this.applyFilters();
    }

    /**
     * Clear all filters and reset to defaults
     */
    clearAllFilters() {
        this.searchQuery = '';
        this.sectorFilter = 'all';
        this.sectorSpecificFilter = '';
        this.sortOption = 'name';
        this.revenueFilter = '';
        
        this.applyFilters();
        console.log('All filters cleared');
    }

    // ============================================================================
    // Pagination
    // ============================================================================

    /**
     * Set current page
     * @param {number} page - Page number (1-based)
     */
    setCurrentPage(page) {
        const totalPages = this.getTotalPages();
        if (page >= 1 && page <= totalPages) {
            this.currentPage = page;
            return true;
        }
        return false;
    }

    /**
     * Get total number of pages
     * @returns {number} Total pages
     */
    getTotalPages() {
        return Math.ceil(this.filteredCustomers.length / this.customersPerPage);
    }

    /**
     * Get customers for current page
     * @returns {Array} Array of customers for current page
     */
    getCurrentPageCustomers() {
        const startIndex = (this.currentPage - 1) * this.customersPerPage;
        const endIndex = startIndex + this.customersPerPage;
        return this.filteredCustomers.slice(startIndex, endIndex);
    }

    /**
     * Get pagination info
     * @returns {Object} Pagination information
     */
    getPaginationInfo() {
        const totalCustomers = this.filteredCustomers.length;
        const totalPages = this.getTotalPages();
        const startIndex = (this.currentPage - 1) * this.customersPerPage + 1;
        const endIndex = Math.min(this.currentPage * this.customersPerPage, totalCustomers);
        
        return {
            currentPage: this.currentPage,
            totalPages,
            totalCustomers,
            startIndex,
            endIndex,
            hasNext: this.currentPage < totalPages,
            hasPrevious: this.currentPage > 1
        };
    }

    // ============================================================================
    // Statistics & Analytics
    // ============================================================================

    /**
     * Get customer statistics
     * @returns {Object} Statistics object
     */
    getCustomerStats() {
        const total = this.customers.length;
        const assigned = this.customers.filter(c => c.sector).length;
        const unassigned = total - assigned;
        
        return {
            total,
            assigned,
            unassigned,
            totalSectors: this.sectors.length,
            selectedCount: this.selectedCustomers.size
        };
    }

    /**
     * Get unique sectors from customers
     * @returns {Array} Array of unique sector names
     */
    getUniqueSectors() {
        return [...new Set(this.customers.map(c => c.sector).filter(Boolean))].sort();
    }

    /**
     * Get sector distribution
     * @returns {Object} Sector distribution statistics
     */
    getSectorDistribution() {
        const distribution = {};
        
        this.customers.forEach(customer => {
            const sector = customer.sector || 'Unassigned';
            distribution[sector] = (distribution[sector] || 0) + 1;
        });
        
        return distribution;
    }

    // ============================================================================
    // Utility Methods
    // ============================================================================

    /**
     * Check if state is empty (no data loaded)
     * @returns {boolean} True if no data loaded
     */
    isEmpty() {
        return this.customers.length === 0 && this.sectors.length === 0;
    }

    /**
     * Get state summary for debugging
     * @returns {Object} State summary
     */
    getStateSummary() {
        return {
            customers: this.customers.length,
            sectors: this.sectors.length,
            filtered: this.filteredCustomers.length,
            selected: this.selectedCustomers.size,
            currentPage: this.currentPage,
            filters: {
                search: this.searchQuery,
                sector: this.sectorFilter,
                specific: this.sectorSpecificFilter,
                sort: this.sortOption,
                revenue: this.revenueFilter
            },
            lastUpdated: this.lastUpdated
        };
    }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CustomerSectorState;
} else {
    // Browser global
    window.CustomerSectorState = CustomerSectorState;
}