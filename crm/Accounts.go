package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	crm "github.com/Leapforce-nl/go_exactonline_new/crm"
)

type AccountBQ struct {
	ClientID               string
	ID                     string
	Accountant             string
	AccountManager         string
	AccountManagerFullName string
	AccountManagerHID      int32
	ActivitySector         string
	ActivitySubSector      string
	AddressLine1           string
	AddressLine2           string
	AddressLine3           string
	//BankAccounts
	Blocked                             bool
	BRIN                                string
	BSN                                 string
	BusinessType                        string
	CanDropShip                         bool
	ChamberOfCommerce                   string
	City                                string
	Classification                      string
	Classification1                     string
	Classification2                     string
	Classification3                     string
	Classification4                     string
	Classification5                     string
	Classification6                     string
	Classification7                     string
	Classification8                     string
	ClassificationDescription           string
	Code                                string
	CodeAtSupplier                      string
	CompanySize                         string
	ConsolidationScenario               byte
	ControlledDate                      bigquery.NullTimestamp
	Costcenter                          string
	CostcenterDescription               string
	CostPaid                            byte
	Country                             string
	CountryName                         string
	Created                             bigquery.NullTimestamp
	Creator                             string
	CreatorFullName                     string
	CreditLinePurchase                  float64
	CreditLineSales                     float64
	Currency                            string
	CustomerSince                       bigquery.NullTimestamp
	DatevCreditorCode                   string
	DatevDebtorCode                     string
	DiscountPurchase                    float64
	DiscountSales                       float64
	Division                            int32
	Document                            string
	DunsNumber                          string
	Email                               string
	EndDate                             bigquery.NullTimestamp
	EstablishedDate                     bigquery.NullTimestamp
	Fax                                 string
	GLAccountPurchase                   string
	GLAccountSales                      string
	GLAP                                string
	GLAR                                string
	GlnNumber                           string
	HasWithholdingTaxSales              bool
	IgnoreDatevWarningMessage           bool
	IntraStatArea                       string
	IntraStatDeliveryTerm               string
	IntraStatSystem                     string
	IntraStatTransactionA               string
	IntraStatTransactionB               string
	IntraStatTransportMethod            string
	InvoiceAccount                      string
	InvoiceAccountCode                  string
	InvoiceAccountName                  string
	InvoiceAttachmentType               int32
	InvoicingMethod                     int32
	IsAccountant                        byte
	IsAgency                            byte
	IsAnonymised                        byte
	IsBank                              bool
	IsCompetitor                        byte
	IsExtraDuty                         bool
	IsMailing                           byte
	IsMember                            bool
	IsPilot                             bool
	IsPurchase                          bool
	IsReseller                          bool
	IsSales                             bool
	IsSupplier                          bool
	Language                            string
	LanguageDescription                 string
	Latitude                            float64
	LeadPurpose                         string
	LeadSource                          string
	Logo                                []byte
	LogoFileName                        string
	LogoThumbnailUrl                    string
	LogoUrl                             string
	Longitude                           float64
	MainContact                         string
	Modified                            bigquery.NullTimestamp
	Modifier                            string
	ModifierFullName                    string
	Name                                string
	OINNumber                           string
	Parent                              string
	PayAsYouEarn                        string
	PaymentConditionPurchase            string
	PaymentConditionPurchaseDescription string
	PaymentConditionSales               string
	PaymentConditionSalesDescription    string
	Phone                               string
	PhoneExtension                      string
	Postcode                            string
	PriceList                           string
	PurchaseCurrency                    string
	PurchaseCurrencyDescription         string
	PurchaseLeadDays                    int32
	PurchaseVATCode                     string
	PurchaseVATCodeDescription          string
	RecepientOfCommissions              bool
	Remarks                             string
	Reseller                            string
	ResellerCode                        string
	ResellerName                        string
	RSIN                                string
	SalesCurrency                       string
	SalesCurrencyDescription            string
	SalesTaxSchedule                    string
	SalesTaxScheduleCode                string
	SalesTaxScheduleDescription         string
	SalesVATCode                        string
	SalesVATCodeDescription             string
	SearchCode                          string
	SecurityLevel                       int32
	SeparateInvPerProject               byte
	SeparateInvPerSubscription          byte
	ShippingLeadDays                    int32
	ShippingMethod                      string
	StartDate                           bigquery.NullTimestamp
	State                               string
	StateName                           string
	Status                              string
	StatusSince                         bigquery.NullTimestamp
	TradeName                           string
	Type                                string
	UniqueTaxpayerReference             string
	VATLiability                        string
	VATNumber                           string
	Website                             string
}

func getAccountBQ(c *crm.Account, clientID string) AccountBQ {
	return AccountBQ{
		clientID,
		c.ID.String(),
		c.Accountant.String(),
		c.AccountManager.String(),
		c.AccountManagerFullName,
		c.AccountManagerHID,
		c.ActivitySector.String(),
		c.ActivitySubSector.String(),
		c.AddressLine1,
		c.AddressLine2,
		c.AddressLine3,
		//c.BankAccounts,
		c.Blocked,
		c.BRIN.String(),
		c.BSN,
		c.BusinessType.String(),
		c.CanDropShip,
		c.ChamberOfCommerce,
		c.City,
		c.Classification,
		c.Classification1.String(),
		c.Classification2.String(),
		c.Classification3.String(),
		c.Classification4.String(),
		c.Classification5.String(),
		c.Classification6.String(),
		c.Classification7.String(),
		c.Classification8.String(),
		c.ClassificationDescription,
		c.Code,
		c.CodeAtSupplier,
		c.CompanySize.String(),
		c.ConsolidationScenario,
		bigquerytools.DateToNullTimestamp(c.ControlledDate),
		c.Costcenter,
		c.CostcenterDescription,
		c.CostPaid,
		c.Country,
		c.CountryName,
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.CreditLinePurchase,
		c.CreditLineSales,
		c.Currency,
		bigquerytools.DateToNullTimestamp(c.CustomerSince),
		c.DatevCreditorCode,
		c.DatevDebtorCode,
		c.DiscountPurchase,
		c.DiscountSales,
		c.Division,
		c.Document.String(),
		c.DunsNumber,
		c.Email,
		bigquerytools.DateToNullTimestamp(c.EndDate),
		bigquerytools.DateToNullTimestamp(c.EstablishedDate),
		c.Fax,
		c.GLAccountPurchase.String(),
		c.GLAccountSales.String(),
		c.GLAP.String(),
		c.GLAR.String(),
		c.GlnNumber,
		c.HasWithholdingTaxSales,
		c.IgnoreDatevWarningMessage,
		c.IntraStatArea,
		c.IntraStatDeliveryTerm,
		c.IntraStatSystem,
		c.IntraStatTransactionA,
		c.IntraStatTransactionB,
		c.IntraStatTransportMethod,
		c.InvoiceAccount.String(),
		c.InvoiceAccountCode,
		c.InvoiceAccountName,
		c.InvoiceAttachmentType,
		c.InvoicingMethod,
		c.IsAccountant,
		c.IsAgency,
		c.IsAnonymised,
		c.IsBank,
		c.IsCompetitor,
		c.IsExtraDuty,
		c.IsMailing,
		c.IsMember,
		c.IsPilot,
		c.IsPurchase,
		c.IsReseller,
		c.IsSales,
		c.IsSupplier,
		c.Language,
		c.LanguageDescription,
		c.Latitude,
		c.LeadPurpose.String(),
		c.LeadSource.String(),
		c.Logo,
		c.LogoFileName,
		c.LogoThumbnailUrl,
		c.LogoUrl,
		c.Longitude,
		c.MainContact.String(),
		bigquerytools.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Name,
		c.OINNumber,
		c.Parent.String(),
		c.PayAsYouEarn,
		c.PaymentConditionPurchase,
		c.PaymentConditionPurchaseDescription,
		c.PaymentConditionSales,
		c.PaymentConditionSalesDescription,
		c.Phone,
		c.PhoneExtension,
		c.Postcode,
		c.PriceList.String(),
		c.PurchaseCurrency,
		c.PurchaseCurrencyDescription,
		c.PurchaseLeadDays,
		c.PurchaseVATCode,
		c.PurchaseVATCodeDescription,
		c.RecepientOfCommissions,
		c.Remarks,
		c.Reseller.String(),
		c.ResellerCode,
		c.ResellerName,
		c.RSIN,
		c.SalesCurrency,
		c.SalesCurrencyDescription,
		c.SalesTaxSchedule.String(),
		c.SalesTaxScheduleCode,
		c.SalesTaxScheduleDescription,
		c.SalesVATCode,
		c.SalesVATCodeDescription,
		c.SearchCode,
		c.SecurityLevel,
		c.SeparateInvPerProject,
		c.SeparateInvPerSubscription,
		c.ShippingLeadDays,
		c.ShippingMethod.String(),
		bigquerytools.DateToNullTimestamp(c.StartDate),
		c.State,
		c.StateName,
		c.Status,
		bigquerytools.DateToNullTimestamp(c.StatusSince),
		c.TradeName,
		c.Type,
		c.UniqueTaxpayerReference,
		c.VATLiability,
		c.VATNumber,
		c.Website,
	}
}

func (client *Client) GetAccountsBQ() (*[]AccountBQ, error) {
	gds, err := client.ExactOnline().CRMClient.GetAccounts()
	if err != nil {
		return nil, err
	}

	if gds == nil {
		return nil, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#Accounts for client %s: %v\n", client.ClientID(), rowCount)

	gdsBQ := []AccountBQ{}

	for _, gd := range *gds {
		gdsBQ = append(gdsBQ, getAccountBQ(&gd, client.ClientID()))
	}

	return &gdsBQ, nil
}

func (client *Client) WriteAccountsBQ(writeToObject *storage.ObjectHandle) (interface{}, error) {
	if writeToObject == nil {
		return nil, nil
	}

	gdsBQ, err := client.GetAccountsBQ()
	if err != nil {
		return nil, err
	}

	if gdsBQ == nil {
		return nil, nil
	}

	ctx := context.Background()

	w := writeToObject.NewWriter(ctx)

	for _, gdBQ := range *gdsBQ {

		b, err := json.Marshal(gdBQ)
		if err != nil {
			return nil, err
		}

		// Write data
		_, err = w.Write(b)
		if err != nil {
			return nil, err
		}

		// Write NewLine
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			return nil, err
		}
	}

	// Close
	err = w.Close()
	if err != nil {
		return nil, err
	}

	return AccountBQ{}, nil
}
