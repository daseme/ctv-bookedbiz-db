# Pipeline Revenue Management System - User Guide

## Overview

The Pipeline Revenue Management System is a comprehensive tool for tracking and managing sales pipeline data with real-time decay tracking. It helps sales managers conduct effective bi-weekly review sessions and maintain accurate pipeline forecasts.

## 🚀 Getting Started

### Accessing the System

1. **Open your web browser** and navigate to your CTV Reports application
2. **Click on "Pipeline"** in the main navigation menu
3. **Select "Pipeline Revenue Management"** from the pipeline section

### System Requirements

- Modern web browser (Chrome, Firefox, Safari, Edge)
- Network access to your CTV database
- Appropriate user permissions for pipeline data

## 📊 Main Interface Overview

### Header Section
- **Title**: "❄️ Pipeline Revenue Management" with Nordic ice theme
- **Session Info**: Shows current review session date and progress
- **Progress Tracker**: Displays how many AEs have been reviewed

### AE Selection Bar
- **Dropdown Menu**: Select which Account Executive to review
- **AE Status Indicators**: Dots show which AEs have been completed
- **Decay Activity Icons**: ⚡ indicates AEs with active decay events

## 🔍 Reviewing an Account Executive

### Step 1: Select an AE
1. **Click the dropdown** labeled "Select an AE to review..."
2. **Choose an AE** from the list
3. **Wait for data to load** (usually 2-3 seconds)

### Step 2: Review AE Statistics
Once selected, you'll see:
- **Total Revenue**: Year-to-date actual revenue
- **YTD Attainment**: Percentage of annual target achieved
- **Avg Deal Size**: Average revenue per deal
- **Decay Activity**: Number of automatic decay events

### Step 3: Review Progress Since Last Session
The system shows:
- **Last Review Date**: When this AE was last reviewed
- **New Revenue Booked**: Revenue added since last review
- **Automatic Decay**: Pipeline reductions applied automatically
- **Decay Events**: Number of automatic adjustments made

## 📅 Monthly Pipeline Cards

### Understanding the Card Layout
The system displays 12 monthly cards arranged in quarters:
- **Q1**: January, February, March + Q1 Summary
- **Q2**: April, May, June + Q2 Summary  
- **Q3**: July, August, September + Q3 Summary
- **Q4**: October, November, December + Q4 Summary

### Month Status Colors
- **🔴 Red (CLOSED)**: Past months - historical data only
- **🟡 Yellow (CURRENT)**: Current month - active for updates
- **🟢 Green (OPEN)**: Future months - planning mode

### Card Information
Each monthly card shows:
- **Booked Revenue**: Actual revenue closed this month
- **Current Pipeline**: Expected revenue not yet booked
- **Budget**: Target revenue for the month
- **Budget Gap**: Difference between target and projected total

### Decay Indicators
- **⚡ Decay Active**: Shows months with automatic decay adjustments
- **🟢 Positive Decay**: Revenue booked, pipeline reduced (good)
- **🔴 Negative Decay**: Revenue lost, pipeline increased (concerning)
- **Days Since Calibration**: Time since last manual review

## ✏️ Editing Pipeline Values

### How to Calibrate Pipeline
1. **Click on any pipeline value** in green or yellow months
2. **Enter the new pipeline amount** in the input field
3. **Press Enter** or **click outside** to save
4. **System confirms** calibration was applied
5. **Decay tracking resets** with new baseline

### Best Practices for Calibration
- **Review decay events first** to understand what changed
- **Consider recent bookings** and cancellations
- **Set realistic expectations** based on deal probability
- **Document significant changes** in review notes

## 👥 Viewing Customer Details

### Opening Customer Modal
1. **Click "View Customers"** on any monthly card
2. **Modal opens** showing three tabs:
   - **Booked Revenue**: Customers who have confirmed deals
   - **Pipeline**: Prospective customers and deals
   - **All Deals**: Combined view of all activity

### Customer Information Displayed
- **Customer Name**: Company or client name
- **Deal Details**: Description, amount, probability
- **Timeline**: Expected close dates, first spot dates
- **Status**: Whether booked or still in pipeline

### Searching and Filtering
- **Search Box**: Type to filter customers by name or deal
- **Tab Navigation**: Switch between booked, pipeline, and all deals
- **Status Indicators**: Visual badges show deal status

## ⚡ Understanding Decay System

### What is Pipeline Decay?
Pipeline decay automatically adjusts pipeline values when:
- **Revenue is booked**: Pipeline decreases (positive decay)
- **Deals are lost**: Pipeline increases (negative decay)
- **Deal probabilities change**: Pipeline adjusts accordingly

### Decay Timeline
1. **Click "Decay Timeline"** button on months with decay activity
2. **View chronological events**: All changes since last calibration
3. **Event details include**:
   - Timestamp of change
   - Type of event (booking, cancellation, etc.)
   - Amount of change
   - Customer involved
   - Who made the change

### Decay Event Types
- **🟢 Revenue Booked**: Customer confirmed deal
- **🔴 Revenue Removed**: Deal cancelled or lost  
- **⚙️ Calibration Reset**: Manual pipeline adjustment
- **✏️ Manual Adjustment**: Other pipeline changes

## 📈 Decay Analytics Dashboard

### Accessing Analytics
1. **Click "View Decay Analytics"** in the progress panel
2. **Dashboard expands** showing comprehensive metrics
3. **Review patterns** across all months

### Key Metrics Displayed
- **Avg Daily Decay**: Average pipeline change per day
- **Avg Decay %**: Percentage of pipeline that decays
- **Total Events**: Number of automatic adjustments
- **Months Tracked**: How many months have decay data

### Using Analytics for Insights
- **High positive decay**: Good sales performance
- **High negative decay**: Pipeline quality issues
- **High volatility**: Unpredictable revenue patterns
- **Low activity**: Stable but potentially stagnant pipeline

## 🔄 Conducting Review Sessions

### Starting a Review Session
1. **Navigate to Pipeline Management**
2. **Click "Start Calibration Session"**
3. **System creates new session** with timestamp
4. **Begin reviewing AEs** one by one

### Review Process
1. **Select first AE** from dropdown
2. **Review decay events** since last session
3. **Examine customer details** for each month
4. **Calibrate pipeline values** as needed
5. **Move to next AE** and repeat

### Session Management
- **Progress tracking**: System shows completion status
- **Session notes**: Add comments for each AE
- **Bulk operations**: Calibrate multiple months at once
- **Session history**: Review past sessions and changes

## 🎯 Best Practices

### Effective Pipeline Management
- **Review every 2 weeks**: Maintain data accuracy
- **Focus on decay events**: Understand what's changing
- **Validate with AEs**: Confirm pipeline assumptions
- **Document decisions**: Track reasoning for changes

### Data Quality Tips
- **Regular calibration**: Don't let decay accumulate
- **Investigate anomalies**: Large decay events need attention
- **Cross-reference with CRM**: Ensure consistency
- **Monitor trends**: Look for patterns in decay data

### Review Session Efficiency
- **Prepare data beforehand**: Review reports before meeting
- **Prioritize problem areas**: Focus on high-decay AEs
- **Set realistic timelines**: Allow adequate time per AE
- **Follow up promptly**: Address issues identified in review

## 🔧 Troubleshooting

### Common Issues

**Data Not Loading**
- Check network connection
- Verify database connectivity
- Refresh browser page
- Contact IT support if persistent

**Pipeline Values Won't Save**
- Ensure you have edit permissions
- Check for network connectivity
- Try refreshing and re-entering
- Contact system administrator

**Decay Events Missing**
- Verify decay system is enabled
- Check if revenue data is updating
- Confirm integration with CRM systems
- Review system logs for errors

**Customer Data Empty**
- Verify data exists in database
- Check date ranges and filters
- Confirm AE name mapping
- Review database permissions

### Getting Help
- **System Documentation**: Check internal wiki/docs
- **IT Support**: Contact for technical issues
- **Sales Operations**: For process questions
- **Training Materials**: Review user training resources

## 📚 System Features Summary

### Core Functionality
- ✅ **Real-time decay tracking**: Automatic pipeline adjustments
- ✅ **Bi-weekly review workflow**: Structured session management
- ✅ **Customer detail views**: Comprehensive deal information
- ✅ **Pipeline calibration**: Easy value adjustments
- ✅ **Decay analytics**: Performance insights and trends

### Advanced Features
- ✅ **Session management**: Track review progress and history
- ✅ **Bulk operations**: Calibrate multiple values at once
- ✅ **Event timeline**: Chronological view of all changes
- ✅ **Status indicators**: Visual cues for data health
- ✅ **Search and filtering**: Find specific customers/deals

### Integration Points
- ✅ **CRM synchronization**: Automatic data updates
- ✅ **Revenue database**: Real-time booking information
- ✅ **Budget systems**: Target and goal tracking
- ✅ **Reporting platform**: Export capabilities

## 🚀 Next Steps

### For New Users
1. **Complete system training** with sales ops team
2. **Review sample data** to understand workflow
3. **Practice with test AE** before live session
4. **Schedule first review session** with manager

### For Managers
1. **Set up regular review cadence** (bi-weekly recommended)
2. **Train team members** on system usage
3. **Establish calibration standards** and processes
4. **Monitor system adoption** and data quality

### System Enhancement
- **Provide feedback** on user experience
- **Request additional features** through proper channels
- **Participate in training updates** as system evolves
- **Share best practices** with other users

---

## 📞 Support Contacts

**Technical Issues**: IT Support Team
**Process Questions**: Sales Operations  
**Training Requests**: Learning & Development
**Feature Requests**: Product Management

*Last Updated: June 2025*
*Version: 2.0*