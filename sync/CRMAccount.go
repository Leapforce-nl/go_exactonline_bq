package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	sync "github.com/leapforce-libraries/go_exactonline_new/sync"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type CRMAccount struct {
	OrganisationID_                     int64
	SoftwareClientLicenceID_            int64
	Created_                            time.Time
	Modified_                           time.Time
	Timestamp                           int64
	Accountant                          string
	AccountManager                      string
	AccountManagerFullName              string
	AccountManagerHID                   int32
	ActivitySector                      string
	ActivitySubSector                   string
	AddressLine1                        string
	AddressLine2                        string
	AddressLine3                        string
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
	ID                                  string
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
	LogoFileName                        string
	LogoThumbnailURL                    string
	LogoURL                             string
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
	SeparateInvoicePerSubscription      byte
	ShippingLeadDays                    int32
	ShippingMethod                      string
	ShowRemarkForSales                  bool
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

func getCRMAccount(c *sync.CRMAccount, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) CRMAccount {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return CRMAccount{
		organisationID,
		softwareClientLicenceID,
		t, t,
		timestamp,
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
		go_bigquery.DateToNullTimestamp(c.ControlledDate),
		c.Costcenter,
		c.CostcenterDescription,
		c.CostPaid,
		c.Country,
		c.CountryName,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.CreditLinePurchase,
		c.CreditLineSales,
		c.Currency,
		go_bigquery.DateToNullTimestamp(c.CustomerSince),
		c.DatevCreditorCode,
		c.DatevDebtorCode,
		c.DiscountPurchase,
		c.DiscountSales,
		c.Division,
		c.Document.String(),
		c.DunsNumber,
		c.Email,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		go_bigquery.DateToNullTimestamp(c.EstablishedDate),
		c.Fax,
		c.GLAccountPurchase.String(),
		c.GLAccountSales.String(),
		c.GLAP.String(),
		c.GLAR.String(),
		c.GlnNumber,
		c.HasWithholdingTaxSales,
		c.ID.String(),
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
		//c.Logo,
		c.LogoFileName,
		c.LogoThumbnailURL,
		c.LogoURL,
		c.Longitude,
		c.MainContact.String(),
		go_bigquery.DateToNullTimestamp(c.Modified),
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
		c.SeparateInvoicePerSubscription,
		c.ShippingLeadDays,
		c.ShippingMethod.String(),
		c.ShowRemarkForSales,
		go_bigquery.DateToNullTimestamp(c.StartDate),
		c.State,
		c.StateName,
		c.Status,
		go_bigquery.DateToNullTimestamp(c.StatusSince),
		c.TradeName,
		c.Type,
		c.UniqueTaxpayerReference,
		c.VATLiability,
		c.VATNumber,
		c.Website,
	}
}

func (service *Service) WriteCRMAccounts(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncCRMAccountsCall(&timestamp)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	maxTimestamp := int64(0)

	for {
		transactionLines, e := call.Do()
		if e != nil {
			return nil, nil, e
		}

		if transactionLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *transactionLines {
			batchRowCount++

			b, err := json.Marshal(getCRMAccount(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#CRMAccounts flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#CRMAccounts: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
