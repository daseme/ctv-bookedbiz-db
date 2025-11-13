/**
 * Customer Sector API - FIXED VERSION
 * Handles all API interactions for customer sector management
 * FIXED: Now properly handles unresolved customers (customer_id = 0)
 */
class CustomerSectorAPI {
    constructor() {
        this.baseUrl = '/api/customer-sector';
        console.log('CustomerSectorAPI initialized');
    }

    async fetchCustomers() {
        try {
            const response = await fetch(`${this.baseUrl}/customers`);
            return await response.json();
        } catch (error) {
            console.error('Error fetching customers:', error);
            return { success: false, error: error.message };
        }
    }

    async fetchSectors() {
        try {
            const response = await fetch(`${this.baseUrl}/sectors`);
            return await response.json();
        } catch (error) {
            console.error('Error fetching sectors:', error);
            return { success: false, error: error.message };
        }
    }

    /**
     * Update customer sector assignment - ENHANCED DEBUG VERSION
     * @param {number} customerId - Customer ID (can be 0 for unresolved customers)
     * @param {string} sectorName - New sector name
     * @returns {Promise<Object>} API response
     */
    async updateCustomerSector(customerId, sectorName) {
        try {
            console.log("=== FRONTEND API CALL DEBUG START ===");
            console.log(`Customer ID: ${customerId}, Sector: ${sectorName}`);
            
            // CRITICAL FIX: For unresolved customers, we need to get the customer name
            let customerName = null;
            
            // If this is an unresolved customer (id = 0), find the customer name
            if (customerId === 0) {
                if (window.customerSectorManager && window.customerSectorManager.state) {
                    const customer = window.customerSectorManager.state.customers.find(c => c.id === 0);
                    if (customer) {
                        customerName = customer.name;
                        console.log(`Found unresolved customer name: ${customerName}`);
                    }
                }
                
                if (!customerName) {
                    console.error('Customer name is required for unresolved customers');
                    throw new Error('Customer name is required for unresolved customers');
                }
            }

            const payload = {
                sector: sectorName
            };

            // CRITICAL FIX: Include customer name for unresolved customers
            if (customerId === 0 && customerName) {
                payload.customer_name = customerName;
            }

            console.log("Payload being sent:", payload);
            console.log("URL:", `${this.baseUrl}/customers/${customerId}/sector`);

            const response = await fetch(`${this.baseUrl}/customers/${customerId}/sector`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload)
            });

            console.log("Response status:", response.status);
            console.log("Response headers:", [...response.headers.entries()]);

            const result = await response.json();
            console.log("Response body:", result);
            
            // IMPORTANT: If a customer was created, update the customer ID in the state
            if (result.success && result.customer_id && customerId === 0) {
                console.log(`Customer created with new ID: ${result.customer_id}`);
                if (window.customerSectorManager && window.customerSectorManager.state) {
                    const updated = window.customerSectorManager.state.updateCustomerId(customerName, result.customer_id);
                    console.log(`State updated: ${updated}`);
                }
            }
            
            console.log("=== FRONTEND API CALL DEBUG END ===");
            return result;
            
        } catch (error) {
            console.error('=== FRONTEND API ERROR ===');
            console.error('Error updating customer sector:', error);
            console.error('Error details:', {
                name: error.name,
                message: error.message,
                stack: error.stack
            });
            return { success: false, error: error.message };
        }
    }

    /**
     * Bulk update customer sectors - FIXED VERSION
     * @param {Array<number>} customerIds - Array of customer IDs
     * @param {string} sectorName - New sector name
     * @returns {Promise<Object>} API response
     */
    async bulkUpdateCustomerSectors(customerIds, sectorName) {
        try {
            // CRITICAL FIX: Build customer updates with names for unresolved customers
            const customerUpdates = [];
            
            if (window.customerSectorManager && window.customerSectorManager.state) {
                for (const customerId of customerIds) {
                    const customer = window.customerSectorManager.state.customers.find(c => c.id === customerId);
                    if (customer) {
                        customerUpdates.push({
                            id: customer.id,
                            name: customer.name,  // Always include name
                            sector: sectorName
                        });
                    }
                }
            }

            const payload = {
                customer_updates: customerUpdates,
                sector: sectorName
            };

            const response = await fetch(`${this.baseUrl}/customers/bulk-sector`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload)
            });

            const result = await response.json();
            
            if (result.success) {
                console.log(`Bulk update: ${result.updated_count} updated, ${result.created_count || 0} created`);
            }
            
            return result;
            
        } catch (error) {
            console.error('Error bulk updating customer sectors:', error);
            return { success: false, error: error.message };
        }
    }

    async createSector(name, group) {
        try {
            const response = await fetch(`${this.baseUrl}/sectors`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    name: name.trim(),
                    description: group ? group.trim() : ''
                })
            });

            return await response.json();
        } catch (error) {
            console.error('Error creating sector:', error);
            return { success: false, error: error.message };
        }
    }

    async checkHealth() {
        try {
            const response = await fetch(`${this.baseUrl}/sectors`);
            return response.ok;
        } catch (error) {
            console.error('API health check failed:', error);
            return false;
        }
    }
}