/**
 * Pipeline UI Components and Rendering
 */

const PipelineUI = {
    /**
     * Show/hide loading state
     */
    setLoading(show) {
        const container = document.querySelector('.pipeline-container');
        if (container) {
            container.classList.toggle('loading', show);
        }
    },
    
    /**
     * Display AE statistics
     */
    displayAEStats(ae) {
        const attainment = ae.ytd_target > 0 ? Math.round((ae.ytd_actual / ae.ytd_target) * 100) : 0;
        const avgDeal = PipelineUtils.formatCurrency(ae.avg_deal_size || 0);
        const totalRevenue = PipelineUtils.formatCurrency(ae.ytd_actual || 0);
        
        this.updateElement('ytd-attainment', `${attainment}%`);
        this.updateElement('avg-deal-size', avgDeal);
        this.updateElement('total-revenue', totalRevenue);
        
        // Show decay activity if available
        if (ae.decay_enabled && ae.decay_analytics) {
            const totalEvents = ae.decay_analytics.overall_metrics?.total_decay_events || 0;
            this.updateElement('decay-activity', totalEvents > 0 ? `${totalEvents} events` : 'None');
            this.showElement('decay-status-stat');
        }
    },
    
    /**
     * Display progress since review
     */
    displayProgressSinceReview(progressData) {
        const progressPanel = document.getElementById('progress-since-review');
        
        if (!progressData) {
            this.showElement('progress-since-review');
            this.updateElement('last-review-date', 'No previous review session');
            this.updateElement('progress-message', 
                'Start your first review session to begin tracking pipeline decay and progress between reviews.');
            
            const statsDiv = document.querySelector('.progress-stats');
            if (statsDiv) statsDiv.style.display = 'none';
            
            const button = document.querySelector('#progress-since-review .btn-calibrate');
            if (button) button.textContent = 'Start First Review Session';
            return;
        }
        
        this.showElement('progress-since-review');
        const statsDiv = document.querySelector('.progress-stats');
        if (statsDiv) statsDiv.style.display = 'flex';
        
        const reviewDate = new Date(progressData.last_review_date);
        const daysAgo = PipelineUtils.getDaysAgo(reviewDate);
        
        this.updateElement('last-review-date', `Last review: ${reviewDate.toLocaleDateString()}`);
        
        const message = `Since our last review ${daysAgo} days ago, we've booked ${PipelineUtils.formatCurrency(progressData.revenue_progress)} additional revenue, with ${progressData.decay_events_count || 0} automatic decay adjustments applied.`;
        this.updateElement('progress-message', message);
        
        this.updateElement('revenue-progress', PipelineUtils.formatCurrency(progressData.revenue_progress));
        this.updateElement('pipeline-reduction', PipelineUtils.formatCurrency(progressData.pipeline_reduction));
        this.updateElement('decay-events-count', progressData.decay_events_count || 0);
    },
    
    /**
     * Display decay analytics
     */
    displayDecayAnalytics(analytics) {
        const analyticsContainer = document.getElementById('decay-analytics');
        const analyticsGrid = document.getElementById('analytics-grid');
        
        if (!analytics || !analytics.overall_metrics) {
            analyticsContainer.classList.remove('visible');
            return;
        }
        
        analyticsContainer.classList.add('visible');
        
        const metrics = analytics.overall_metrics;
        
        analyticsGrid.innerHTML = `
            <div class="analytics-card">
                <div class="analytics-value">${PipelineUtils.formatCurrency(metrics.avg_daily_decay_rate || 0)}</div>
                <div class="analytics-label">Avg Daily Decay</div>
            </div>
            <div class="analytics-card">
                <div class="analytics-value">${(metrics.avg_decay_percentage || 0).toFixed(1)}%</div>
                <div class="analytics-label">Avg Decay %</div>
            </div>
            <div class="analytics-card">
                <div class="analytics-value">${metrics.total_decay_events || 0}</div>
                <div class="analytics-label">Total Events</div>
            </div>
            <div class="analytics-card">
                <div class="analytics-value">${metrics.months_analyzed || 0}</div>
                <div class="analytics-label">Months Tracked</div>
            </div>
        `;
    },
    
    /**
     * Create a month card component
     */
    createMonthCard(month) {
        const card = document.createElement('div');
        card.className = 'month-card';

        const monthStatus = PipelineUtils.getMonthStatus(month.month);
        let statusText = monthStatus.charAt(0).toUpperCase() + monthStatus.slice(1);
        
        card.classList.add(`${monthStatus}-month`);
        
        // Add decay indicators
        let decayIndicator = '';
        let decaySection = '';
        
        if (month.has_decay_activity) {
            card.classList.add('has-decay');
            
            const totalDecay = month.total_decay || 0;
            const decayClass = PipelineUtils.getDecayClass(totalDecay);
            card.classList.add(decayClass);
            
            decayIndicator = `<div class="decay-indicator active ${decayClass.replace('decay-', '')}"></div>`;
            
            decaySection = `
                <div class="decay-summary">
                    <div class="gap-row">
                        <span>Total Decay:</span>
                        <span class="gap-amount ${totalDecay <= 0 ? 'positive' : 'negative'}">
                            ${totalDecay <= 0 ? '+' : '-'}${PipelineUtils.formatCurrency(Math.abs(totalDecay))}
                        </span>
                    </div>
                    <div class="gap-row">
                        <span>Days Since Cal:</span>
                        <span>${month.days_since_calibration || 0} days</span>
                    </div>
                    <div class="decay-event-count">
                        ${month.decay_events_count || 0} decay events
                        ${month.decay_events_count > 0 ? `<br><a href="#" class="decay-timeline-link" onclick="DecayTimeline.show('${month.month}')">View Timeline</a>` : ''}
                    </div>
                </div>
            `;
        }
        
        card.innerHTML = `
            ${decayIndicator}
            <div class="month-header">
                <div class="month-title">${month.month_display}</div>
                <div class="month-status status-${monthStatus}">
                    ${statusText}
                </div>
            </div>
            
            <div class="revenue-metrics">
                <div class="metric-row">
                    <span class="metric-label">
                        Booked Revenue:
                        ${month.has_decay_activity && month.total_decay < 0 ? '<span class="decay-badge positive">AUTO</span>' : ''}
                    </span>
                    <span class="metric-value booked">${PipelineUtils.formatCurrency(month.booked_revenue)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">
                        Current Pipeline:
                        ${month.has_decay_activity ? '<span class="decay-badge neutral">LIVE</span>' : ''}
                    </span>
                    <span class="metric-value pipeline ${monthStatus === 'closed' ? 'disabled' : ''}"
                          ${monthStatus !== 'closed' ? `onclick="PipelineController.editPipeline('${month.month}', this)"` : ''}
                          data-original="${month.current_pipeline}">
                        ${PipelineUtils.formatCurrency(month.current_pipeline)}
                    </span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Budget (ref):</span>
                    <span class="metric-value budget">${PipelineUtils.formatCurrency(month.budget)}</span>
                </div>
            </div>
            
            <div class="gap-analysis">
                <div class="gap-row gap-secondary">
                    <span>Budget Gap:</span>
                    <span class="gap-amount ${month.budget_gap >= 0 ? 'positive' : 'negative'}">
                        ${month.budget_gap >= 0 ? '+' : '-'}${PipelineUtils.formatCurrency(Math.abs(month.budget_gap))}
                    </span>
                </div>
            </div>
            
            ${decaySection}
            
            <div class="card-actions" style="margin-top: 16px;">
                <button class="btn btn-primary btn-sm" onclick="CustomerModal.open('${month.month}')">
                    View Customers
                </button>
                ${month.has_decay_activity ? `
                    <button class="btn btn-sm" onclick="DecayTimeline.show('${month.month}')"
                            style="background: var(--decay-neutral); color: var(--nord6); border: none; padding: 8px 16px; border-radius: 8px; cursor: pointer; font-weight: 600; margin-left: 8px;">
                        Decay Timeline
                    </button>
                ` : ''}
            </div>
        `;
        
        return card;
    },
    
    /**
     * Create a quarter card component
     */
    createQuarterCard(quarter) {
        const card = document.createElement('div');
        card.className = 'quarter-card';

        card.innerHTML = `
            <div class="month-header">
                <div class="month-title">${quarter.quarter_name}</div>
                <div class="month-status">
                    QUARTER TOTAL
                </div>
            </div>

            <div class="revenue-metrics">
                <div class="metric-row">
                    <span class="metric-label">Booked Revenue:</span>
                    <span class="metric-value booked">${PipelineUtils.formatCurrency(quarter.booked_revenue)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Current Pipeline:</span>
                    <span class="metric-value" style="color: var(--nord8);">
                        ${PipelineUtils.formatCurrency(quarter.current_pipeline)}
                    </span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Budget (ref):</span>
                    <span class="metric-value budget">${PipelineUtils.formatCurrency(quarter.budget)}</span>
                </div>
            </div>

            <div class="gap-analysis">
                <div class="gap-row gap-secondary">
                    <span>Budget Gap:</span>
                    <span class="gap-amount ${quarter.budget_gap >= 0 ? 'positive' : 'negative'}">
                        ${quarter.budget_gap >= 0 ? '+' : '-'}${PipelineUtils.formatCurrency(Math.abs(quarter.budget_gap))}
                    </span>
                </div>
            </div>

            <div class="quarter-status" style="margin-top: 16px; text-align: center; font-size: 14px;">
                <strong>${quarter.month_count || 3} months</strong>
                <div style="margin-top: 4px; ${this.getQuarterStatusStyle(quarter.quarter_name)}">
                    ${this.determineQuarterCompletion(quarter.quarter_name)}
                </div>
            </div>
        `;

        return card;
    },
    
    /**
     * Determine quarter completion status
     */
    determineQuarterCompletion(quarterName) {
        const currentDate = new Date();
        const currentMonth = currentDate.getMonth() + 1;
        const currentYear = currentDate.getFullYear();
        
        const quarterMap = {
            [`Q1 ${currentYear}`]: { startMonth: 1, endMonth: 3, year: currentYear },
            [`Q2 ${currentYear}`]: { startMonth: 4, endMonth: 6, year: currentYear },
            [`Q3 ${currentYear}`]: { startMonth: 7, endMonth: 9, year: currentYear },
            [`Q4 ${currentYear}`]: { startMonth: 10, endMonth: 12, year: currentYear }
        };
        
        const quarter = quarterMap[quarterName];
        if (!quarter) return 'Unknown';
        
        if (currentYear > quarter.year || 
            (currentYear === quarter.year && currentMonth > quarter.endMonth)) {
            return '✓ Complete';
        }
        else if (currentYear === quarter.year && 
                 currentMonth >= quarter.startMonth && 
                 currentMonth <= quarter.endMonth) {
            return 'In Progress';
        }
        else {
            return 'Planned';
        }
    },
    
    /**
     * Get quarter status styling
     */
    getQuarterStatusStyle(quarterName) {
        const currentDate = new Date();
        const currentMonth = currentDate.getMonth() + 1;
        const currentYear = currentDate.getFullYear();
        
        const quarterMap = {
            [`Q1 ${currentYear}`]: { startMonth: 1, endMonth: 3, year: currentYear },
            [`Q2 ${currentYear}`]: { startMonth: 4, endMonth: 6, year: currentYear },
            [`Q3 ${currentYear}`]: { startMonth: 7, endMonth: 9, year: currentYear },
            [`Q4 ${currentYear}`]: { startMonth: 10, endMonth: 12, year: currentYear }
        };
        
        const quarter = quarterMap[quarterName];
        if (!quarter) return 'color: var(--nord2);';
        
        if (currentYear > quarter.year || 
            (currentYear === quarter.year && currentMonth > quarter.endMonth)) {
            return 'color: var(--nord14); font-weight: 600;';
        }
        else if (currentYear === quarter.year && 
                 currentMonth >= quarter.startMonth && 
                 currentMonth <= quarter.endMonth) {
            return 'color: var(--nord8); font-weight: 500;';
        }
        else {
            return 'color: var(--nord15); font-weight: 400;';
        }
    },
    
    /**
     * Display monthly cards grid
     */
    displayMonthlyCards(monthlyData, quarterlyData = []) {
        const grid = document.getElementById('monthly-grid');
        const summaryDisplay = document.getElementById('month-summary-display');

        if (!monthlyData || monthlyData.length === 0) {
            grid.style.display = 'none';
            summaryDisplay.style.display = 'flex';
            summaryDisplay.innerHTML = '<div>❄️ No monthly data available for this Account Executive</div>';
            return;
        }

        grid.style.display = 'grid';
        summaryDisplay.style.display = 'none';
        grid.innerHTML = '';

        // Define quarters with their months arranged in 4-column rows
        const currentYear = new Date().getFullYear();
        const quarters = [
            { name: `Q1 ${currentYear}`, months: [`${currentYear}-01`, `${currentYear}-02`, `${currentYear}-03`] },
            { name: `Q2 ${currentYear}`, months: [`${currentYear}-04`, `${currentYear}-05`, `${currentYear}-06`] },
            { name: `Q3 ${currentYear}`, months: [`${currentYear}-07`, `${currentYear}-08`, `${currentYear}-09`] },
            { name: `Q4 ${currentYear}`, months: [`${currentYear}-10`, `${currentYear}-11`, `${currentYear}-12`] }
        ];

        quarters.forEach(quarter => {
            // Add 3 monthly card slots for this quarter row
            quarter.months.forEach(monthStr => {
                const month = monthlyData.find(m => m.month === monthStr);
                if (month) {
                    const card = this.createMonthCard(month);
                    grid.appendChild(card);
                } else {
                    // Add empty placeholder if month data is missing
                    const emptyCard = document.createElement('div');
                    emptyCard.className = 'month-card empty-slot';
                    emptyCard.innerHTML = '<div class="month-header"><div class="month-title">No Data</div></div>';
                    grid.appendChild(emptyCard);
                }
            });

            // Add quarterly summary card as the 4th column
            const quarterData = quarterlyData.find(q => q.quarter_name === quarter.name);
            if (quarterData) {
                const quarterCard = this.createQuarterCard(quarterData);
                grid.appendChild(quarterCard);
            } else {
                // Add empty placeholder if quarter data is missing
                const emptyCard = document.createElement('div');
                emptyCard.className = 'quarter-card empty-slot';
                emptyCard.innerHTML = '<div class="month-header"><div class="month-title">No Quarter Data</div></div>';
                grid.appendChild(emptyCard);
            }
        });
    },
    
    /**
     * Show AE data sections
     */
    showAEData() {
        this.showElement('ae-stats', 'flex');
        this.showElement('monthly-grid', 'grid');
        this.hideElement('month-summary-display');
    },
    
    /**
     * Hide AE data sections
     */
    hideAEData() {
        this.hideElement('ae-stats');
        this.hideElement('monthly-grid');
        this.showElement('month-summary-display', 'flex');
        this.hideElement('progress-since-review');
        document.getElementById('decay-analytics')?.classList.remove('visible');
    },
    
    /**
     * Update completion status dots
     */
    updateCompletionStatus(completedAEs = []) {
        const dots = document.querySelectorAll('.status-dot');
        
        dots.forEach(dot => {
            const aeId = dot.dataset.aeId;
            if (completedAEs.includes(aeId)) {
                dot.classList.add('completed');
            }
        });
        
        this.updateElement('completed-count', completedAEs.length);
    },
    
    /**
     * Show alert message
     */
    showAlert(type, message) {
        const container = document.getElementById('alert-container');
        if (!container) return;
        
        const alert = document.createElement('div');
        alert.className = `alert alert-${type}`;
        alert.textContent = message;
        
        container.appendChild(alert);
        
        // Auto-remove after configured duration
        setTimeout(() => {
            if (alert.parentNode) {
                alert.parentNode.removeChild(alert);
            }
        }, PIPELINE_CONFIG.ui.animations.alertDuration);
    },
    
    /**
     * Utility functions for element manipulation
     */
    updateElement(id, content) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = content;
        }
    },
    
    showElement(id, displayType = 'block') {
        const element = document.getElementById(id);
        if (element) {
            element.style.display = displayType;
        }
    },
    
    hideElement(id) {
        const element = document.getElementById(id);
        if (element) {
            element.style.display = 'none';
        }
    }
};

// Export for use in other modules
window.PipelineUI = PipelineUI;