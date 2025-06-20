/**
 * Pipeline API Service Layer
 */

const PipelineAPI = {
    /**
     * Generic API request handler
     */
    async request(url, options = {}) {
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
            }
        };
        
        const mergedOptions = { ...defaultOptions, ...options };
        
        try {
            const response = await fetch(url, mergedOptions);
            const result = await response.json();
            
            if (!response.ok) {
                throw new Error(result.error || `HTTP error! status: ${response.status}`);
            }
            
            return result;
        } catch (error) {
            console.error('API request failed:', error);
            throw error;
        }
    },
    
    /**
     * Load AE summary data with decay information
     */
    async loadAEData(aeId) {
        const url = PipelineUtils.buildApiUrl('aeData', { aeId });
        return await this.request(url);
    },
    
    /**
     * Load customer data for a specific month
     */
    async loadCustomerData(aeId, month) {
        const url = PipelineUtils.buildApiUrl('customers', { aeId, month });
        return await this.request(url);
    },
    
    /**
     * Load decay timeline for a specific month
     */
    async loadDecayTimeline(aeId, month) {
        const url = PipelineUtils.buildApiUrl('decayTimeline', { aeId, month });
        return await this.request(url);
    },
    
    /**
     * Submit pipeline calibration
     */
    async calibratePipeline(aeId, month, pipelineValue, sessionId = null) {
        const url = PipelineUtils.buildApiUrl('calibration');
        const data = {
            ae_id: aeId,
            month: month,
            pipeline_value: pipelineValue,
            calibrated_by: 'manual_review',
            session_id: sessionId || `session_${Date.now()}`
        };
        
        return await this.request(url, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    },
    
    /**
     * Create a new review snapshot
     */
    async createReviewSnapshot(aeId) {
        const url = PipelineUtils.buildApiUrl('reviewSnapshot', { aeId });
        return await this.request(url, {
            method: 'POST'
        });
    }
};

// Export for use in other modules
window.PipelineAPI = PipelineAPI;