/**
 * Pipeline Configuration and Constants
 */

// Application Configuration
const PIPELINE_CONFIG = {
    api: {
        baseUrl: '/api',
        endpoints: {
            aeData: '/ae/{aeId}/summary',
            customers: '/customers/{aeId}/{month}',
            decayTimeline: '/pipeline/decay/timeline/{aeId}/{month}',
            calibration: '/pipeline/decay/calibration',
            reviewSnapshot: '/review/snapshot/{aeId}'
        }
    },
    
    ui: {
        animations: {
            fadeIn: 300,
            slideIn: 400,
            alertDuration: 5000
        },
        
        formats: {
            currency: {
                style: 'currency',
                currency: 'USD',
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            },
            
            date: {
                short: { month: 'short', day: 'numeric', year: 'numeric' },
                long: { month: 'long', year: 'numeric' }
            }
        }
    },
    
    decay: {
        eventTypes: {
            REVENUE_BOOKED: 'revenue_booked',
            REVENUE_REMOVED: 'revenue_removed',
            CALIBRATION_RESET: 'calibration_reset',
            MANUAL_ADJUSTMENT: 'manual_adjustment'
        },
        
        eventIcons: {
            revenue_booked: '💰',
            revenue_removed: '❌',
            calibration_reset: '⚙️',
            manual_adjustment: '✏️'
        },
        
        classes: {
            positive: 'decay-positive',
            negative: 'decay-negative',
            neutral: 'decay-neutral'
        }
    },
    
    status: {
        month: {
            CLOSED: 'closed',
            CURRENT: 'current',
            OPEN: 'open'
        },
        
        deal: {
            BOOKED: 'booked',
            PIPELINE: 'pipeline'
        }
    }
};

// Utility Functions
const PipelineUtils = {
    /**
     * Format currency amount
     */
    formatCurrency: function(amount) {
        if (amount === null || amount === undefined) return '$0';
        return new Intl.NumberFormat('en-US', PIPELINE_CONFIG.ui.formats.currency).format(amount);
    },
    
    /**
     * Format date
     */
    formatDate: function(date, format = 'short') {
        if (!date) return 'N/A';
        const dateObj = new Date(date);
        return dateObj.toLocaleDateString('en-US', PIPELINE_CONFIG.ui.formats.date[format]);
    },
    
    /**
     * Format month/year from month string
     */
    formatMonthYear: function(monthStr) {
        if (!monthStr) return 'N/A';
        const [year, month] = monthStr.split('-');
        const date = new Date(year, month - 1);
        return date.toLocaleDateString('en-US', PIPELINE_CONFIG.ui.formats.date.long);
    },
    
    /**
     * Get days between dates
     */
    getDaysAgo: function(date) {
        const now = new Date();
        const diff = now.getTime() - new Date(date).getTime();
        return Math.floor(diff / (1000 * 60 * 60 * 24));
    },
    
    /**
     * Generate API URL with parameters
     */
    buildApiUrl: function(endpoint, params = {}) {
        let url = PIPELINE_CONFIG.api.baseUrl + PIPELINE_CONFIG.api.endpoints[endpoint];
        
        // Replace parameters in URL
        Object.keys(params).forEach(key => {
            url = url.replace(`{${key}}`, params[key]);
        });
        
        return url;
    },
    
    /**
     * Determine month status
     */
    getMonthStatus: function(monthStr) {
        const currentDate = new Date();
        const currentMonth = currentDate.getMonth() + 1;
        const currentYear = currentDate.getFullYear();
        const [monthYear, monthNum] = monthStr.split('-');
        const monthNumber = parseInt(monthNum);
        
        if (parseInt(monthYear) < currentYear || 
            (parseInt(monthYear) === currentYear && monthNumber < currentMonth)) {
            return PIPELINE_CONFIG.status.month.CLOSED;
        } else if (parseInt(monthYear) === currentYear && monthNumber === currentMonth) {
            return PIPELINE_CONFIG.status.month.CURRENT;
        } else {
            return PIPELINE_CONFIG.status.month.OPEN;
        }
    },
    
    /**
     * Get decay class based on value
     */
    getDecayClass: function(decayValue) {
        if (decayValue < 0) return PIPELINE_CONFIG.decay.classes.positive;
        if (decayValue > 0) return PIPELINE_CONFIG.decay.classes.negative;
        return PIPELINE_CONFIG.decay.classes.neutral;
    },
    
    /**
     * Debounce function calls
     */
    debounce: function(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
};

// Export for use in other modules
window.PIPELINE_CONFIG = PIPELINE_CONFIG;
window.PipelineUtils = PipelineUtils;