/**
 * Main Pipeline Controller - Orchestrates all components
 */

const PipelineController = {
    // Application state
    currentAE: null,
    currentMonthlyData: [],
    currentQuarterlyData: [],
    currentDecayAnalytics: null,
    sessionData: null,
    originalPipelineValues: {},
    
    /**
     * Initialize the application
     */
    init(sessionData) {
        console.log('Enhanced Pipeline Revenue Management with Decay loading...');
        
        this.sessionData = sessionData;
        this.setupEventListeners();
        PipelineUI.updateCompletionStatus(sessionData.completed_aes || []);
        
        console.log('Enhanced Pipeline Revenue Management with Decay initialized');
    },
    
    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // AE selector change
        const aeSelector = document.getElementById('ae-selector');
        if (aeSelector) {
            aeSelector.addEventListener('change', (e) => {
                const aeId = e.target.value;
                if (aeId) {
                    this.loadAEData(aeId);
                } else {
                    this.hideAEData();
                }
            });
        }
        
        // Close modals when clicking outside
        window.addEventListener('click', (event) => {
            const customerModal = document.getElementById('customer-modal');
            const decayModal = document.getElementById('decay-timeline-modal');
            
            if (event.target === customerModal) {
                CustomerModal.close();
            }
            if (event.target === decayModal) {
                DecayTimeline.close();
            }
        });
        
        // Handle escape key for modals
        document.addEventListener('keydown', (event) => {
            if (event.key === 'Escape') {
                CustomerModal.close();
                DecayTimeline.close();
            }
        });
    },
    
    /**
     * Load AE data with enhanced error handling
     */
    async loadAEData(aeId) {
        try {
            PipelineUI.setLoading(true);
            
            const result = await PipelineAPI.loadAEData(aeId);
            
            if (result.success) {
                this.currentAE = result.data.ae_info;
                this.currentMonthlyData = result.data.monthly_summary;
                this.currentQuarterlyData = result.data.quarterly_summary || [];
                this.currentDecayAnalytics = result.data.decay_analytics || null;
                
                PipelineUI.displayAEStats(this.currentAE);
                PipelineUI.displayProgressSinceReview(result.data.progress_since_review);
                PipelineUI.displayMonthlyCards(this.currentMonthlyData, this.currentQuarterlyData);
                PipelineUI.displayDecayAnalytics(this.currentDecayAnalytics);
                PipelineUI.showAEData();
                
                // Store original pipeline values for change tracking
                this.originalPipelineValues = {};
                this.currentMonthlyData.forEach(month => {
                    this.originalPipelineValues[month.month] = month.current_pipeline;
                });
                
            } else {
                PipelineUI.showAlert('error', result.error || 'Failed to load AE data');
            }
        } catch (error) {
            console.error('Error loading AE data:', error);
            PipelineUI.showAlert('error', 'Failed to load AE data');
        } finally {
            PipelineUI.setLoading(false);
        }
    },
    
    /**
     * Hide AE data
     */
    hideAEData() {
        PipelineUI.hideAEData();
        this.currentAE = null;
        this.currentMonthlyData = [];
        this.currentQuarterlyData = [];
        this.currentDecayAnalytics = null;
        this.originalPipelineValues = {};
    },
    
    /**
     * Enhanced pipeline editing with decay awareness
     */
    async editPipeline(month, element) {
        if (!this.currentAE) {
            PipelineUI.showAlert('error', 'No AE selected');
            return;
        }
        
        const currentValue = parseFloat(element.dataset.original) || 0;
        const input = document.createElement('input');
        input.type = 'number';
        input.className = 'pipeline-input';
        input.value = currentValue;
        input.step = '1000';
        
        element.classList.add('editing');
        element.innerHTML = '';
        element.appendChild(input);
        
        input.focus();
        input.select();
        
        const saveValue = async () => {
            const newValue = parseFloat(input.value) || 0;
            
            if (newValue !== currentValue) {
                try {
                    const sessionId = `session_${Date.now()}`;
                    const result = await PipelineAPI.calibratePipeline(
                        this.currentAE.ae_id, 
                        month, 
                        newValue, 
                        sessionId
                    );
                    
                    if (result.success) {
                        // Update the display
                        element.dataset.original = newValue;
                        element.innerHTML = PipelineUtils.formatCurrency(newValue);
                        element.classList.remove('editing');
                        
                        // Refresh the data to show decay information
                        await this.loadAEData(this.currentAE.ae_id);
                        
                        PipelineUI.showAlert('calibration', `Pipeline calibrated: ${PipelineUtils.formatCurrency(newValue)}`);
                    } else {
                        this.resetPipelineInput(element, currentValue);
                        PipelineUI.showAlert('error', result.error || 'Failed to calibrate pipeline');
                    }
                } catch (error) {
                    console.error('Error calibrating pipeline:', error);
                    this.resetPipelineInput(element, currentValue);
                    PipelineUI.showAlert('error', 'Network error calibrating pipeline');
                }
            } else {
                this.resetPipelineInput(element, currentValue);
            }
        };
        
        const cancelEdit = () => {
            this.resetPipelineInput(element, currentValue);
        };
        
        input.addEventListener('blur', saveValue);
        input.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                saveValue();
            } else if (e.key === 'Escape') {
                cancelEdit();
            }
        });
    },
    
    /**
     * Reset pipeline input to original value
     */
    resetPipelineInput(element, originalValue) {
        element.innerHTML = PipelineUtils.formatCurrency(originalValue);
        element.classList.remove('editing');
    },
    
    /**
     * Start bulk calibration session
     */
    startBulkCalibration() {
        PipelineUI.showAlert('calibration', 'Bulk calibration feature coming soon!');
    },
    
    /**
     * Toggle decay analytics view
     */
    viewDecayAnalytics() {
        const analyticsContainer = document.getElementById('decay-analytics');
        if (analyticsContainer) {
            if (analyticsContainer.classList.contains('visible')) {
                analyticsContainer.classList.remove('visible');
            } else {
                analyticsContainer.classList.add('visible');
                analyticsContainer.scrollIntoView({ behavior: 'smooth' });
            }
        }
    },
    
    /**
     * Create new review snapshot
     */
    async createReviewSnapshot() {
        if (!this.currentAE) {
            PipelineUI.showAlert('error', 'No AE selected');
            return;
        }
        
        try {
            const result = await PipelineAPI.createReviewSnapshot(this.currentAE.ae_id);
            
            if (result.success) {
                PipelineUI.showAlert('success', 'Review snapshot created successfully');
                await this.loadAEData(this.currentAE.ae_id);
            } else {
                PipelineUI.showAlert('error', result.error || 'Failed to create review snapshot');
            }
        } catch (error) {
            console.error('Error creating review snapshot:', error);
            PipelineUI.showAlert('error', 'Network error creating review snapshot');
        }
    },
    
    /**
     * Get current AE (for use by other modules)
     */
    getCurrentAE() {
        return this.currentAE;
    },
    
    /**
     * Get current monthly data (for use by other modules)
     */
    getCurrentMonthlyData() {
        return this.currentMonthlyData;
    },
    
    /**
     * Get current quarterly data (for use by other modules)
     */
    getCurrentQuarterlyData() {
        return this.currentQuarterlyData;
    }
};

// Export for use in other modules and global access
window.PipelineController = PipelineController; 