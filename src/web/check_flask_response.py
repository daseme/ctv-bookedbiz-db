import requests
import re

def check_management_report():
    """Check the management report response."""
    
    print("🔍 Checking Management Report Response...")
    
    try:
        # Make request
        r = requests.get('http://localhost:5000/management-report')
        print(f"Status Code: {r.status_code}")
        
        if r.status_code != 200:
            print(f"❌ Bad status code: {r.status_code}")
            return
        
        content = r.text
        print(f"Response Length: {len(content)} characters")
        
        # Look for the Total Revenue section
        total_revenue_pattern = r'Total Revenue.*?font-size: 24px.*?>\$([^<]+)'
        total_revenue_match = re.search(total_revenue_pattern, content, re.DOTALL)
        
        if total_revenue_match:
            revenue_value = total_revenue_match.group(1).strip()
            print(f"✅ Total Revenue found: ${revenue_value}")
        else:
            print("❌ Total Revenue value not found")
            
            # Look for the Total Revenue section more broadly
            revenue_section_pattern = r'Total Revenue.*?24px.*?>(.*?)</div>'
            revenue_section_match = re.search(revenue_section_pattern, content, re.DOTALL)
            
            if revenue_section_match:
                section_content = revenue_section_match.group(1).strip()
                print(f"Total Revenue section content: '{section_content}'")
            else:
                print("No Total Revenue section found at all")
        
        # Count dollar signs and values
        dollar_signs = content.count('$')
        print(f"Dollar signs in response: {dollar_signs}")
        
        # Look for any revenue values
        revenue_values = re.findall(r'\$[\d,]+', content)
        print(f"Revenue values found: {revenue_values[:10]}")  # First 10
        
        # Check if template variables are being rendered
        if '{{' in content or '}}' in content:
            print("⚠️ Unrendered template variables found!")
            template_vars = re.findall(r'\{\{[^}]+\}\}', content)
            print(f"Unrendered variables: {template_vars[:5]}")
        
        # Look for specific management report sections
        if 'Management Performance Report' in content:
            print("✅ Management report title found")
        else:
            print("❌ Management report title not found")
            
        if 'Company Performance Overview' in content:
            print("✅ Company overview section found")
        else:
            print("❌ Company overview section not found")
        
    except Exception as e:
        print(f"❌ Error checking response: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_management_report() 