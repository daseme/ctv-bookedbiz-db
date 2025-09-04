/**
 * Customer Sector Management API Layer
 * 
 * Handles all communication with the backend API following clean architecture principles.
 * Provides a clean interface for data operations with proper error handling.
 */

class CustomerSectorAPI {
    constructor() {
        this.baseUrl = '/api/customer-sector';
    }

    /**
     * Fetch all customers for sector management
     * @returns {Promise<Object>} API response with customers data
     */
    async fetchCustomers() {
        try {
            const response = await fetch(`${this.baseUrl}/customers`);
            const result = await response.json();
            
            if (result.success) {
                console.log('Loaded', result.data.length, 'Internal Ad Sales customers (excluding WorldLink)');
                return { success: true, data: result.data };
            } else {
                throw new Error(result.error || 'Failed to fetch customers');
            }
        } catch (error) {
            console.error('Error fetching customers:', error);
            return {
                success: false,
                error: error.message,
                data: []
            };
        }
    }

    /**
     * Fetch all available sectors
     * @returns {Promise<Object>} API response with sectors data
     */
    async fetchSectors() {
        try {
            const response = await fetch(`${this.baseUrl}/sectors`);
            const result = await response.json();
            
            if (result.success) {
                console.log('Loaded', result.data.length, 'sectors');
                return { success: true, data: result.data };
            } else {
                throw new Error(result.error || 'Failed to fetch sectors');
            }
        } catch (error) {
            console.error('Error fetching sectors:', error);
            return {
                success: false,
                error: error.message,
                data: []
            };
        }
    }

    /**
     * Update a customer's sector assignment
     * @param {number} customerId - Customer ID to update
     * @param {string|null} newSector - New sector name (null for unassigned)
     * @returns {Promise<Object>} API response
     */
    async updateCustomerSector(customerId, newSector) {
        if (!customerId) {
            return { success: false, error: 'Customer ID is required' };
        }

        try {
            const response = await fetch(`${this.baseUrl}/customers/${customerId}/sector`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ sector: newSector })
            });
            
            const result = await response.json();
            
            if (result.success) {
                const sectorName = newSector || 'Unassigned';
                return {
                    success: true,
                    message: `Customer sector updated to ${sectorName}`,
                    data: result.data
                };
            } else {
                throw new Error(result.error || 'Update failed');
            }
        } catch (error) {
            console.error('Error updating customer sector:', error);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Create a new sector
     * @param {string} name - Sector name
     * @param {string} [group] - Optional sector group
     * @returns {Promise<Object>} API response with new sector data
     */
    async createSector(name, group = '') {
        if (!name || !name.trim()) {
            return { success: false, error: 'Sector name is required' };
        }

        try {
            const response = await fetch(`${this.baseUrl}/sectors`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ 
                    name: name.trim(),
                    description: group.trim() || null
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                return {
                    success: true,
                    message: `Sector "${name}" added successfully`,
                    data: {
                        id: result.data.id,
                        name: result.data.name,
                        description: result.data.description,
                        color: this._generateRandomSectorColor()
                    }
                };
            } else {
                throw new Error(result.error || 'Failed to add sector');
            }
        } catch (error) {
            console.error('Error adding sector:', error);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Bulk update customer sectors
     * @param {number[]} customerIds - Array of customer IDs
     * @param {string} sectorName - Sector name to assign
     * @returns {Promise<Object>} API response
     */
    async bulkUpdateCustomerSectors(customerIds, sectorName) {
        if (!customerIds || customerIds.length === 0) {
            return { success: false, error: 'No customers selected' };
        }

        if (!sectorName) {
            return { success: false, error: 'Please select a sector to assign' };
        }

        try {
            const response = await fetch(`${this.baseUrl}/customers/bulk-sector`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    customer_ids: customerIds,
                    sector: sectorName
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                return {
                    success: true,
                    message: result.message || `Updated ${customerIds.length} customers`,
                    data: result.data
                };
            } else {
                throw new Error(result.error || 'Bulk update failed');
            }
        } catch (error) {
            console.error('Error in bulk update:', error);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Generate a random color for new sectors
     * @private
     * @returns {string} Hex color code
     */
    _generateRandomSectorColor() {
        const colors = [
            '#88c0d0', '#81a1c1', '#5e81ac', '#bf616a', 
            '#d08770', '#ebcb8b', '#a3be8c', '#b48ead'
        ];
        return colors[Math.floor(Math.random() * colors.length)];
    }

    /**
     * Check API health
     * @returns {Promise<boolean>} True if API is responsive
     */
    async checkHealth() {
        try {
            const response = await fetch('/health', { method: 'HEAD' });
            return response.ok;
        } catch (error) {
            console.warn('API health check failed:', error);
            return false;
        }
    }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CustomerSectorAPI;
} else {
    // Browser global
    window.CustomerSectorAPI = CustomerSectorAPI;
}