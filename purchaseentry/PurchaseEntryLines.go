package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	purchaseentry "github.com/leapforce-libraries/go_exactonline_new/purchaseentry"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type PurchaseEntryLine struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	ID                         string
	AmountDC                   float64
	AmountFC                   float64
	Asset                      string
	AssetDescription           string
	CostCenter                 string
	CostCenterDescription      string
	CostUnit                   string
	CostUnitDescription        string
	Description                string
	Division                   int32
	EntryID                    string
	From                       bigquery.NullTimestamp
	GLAccount                  string
	GLAccountCode              string
	GLAccountDescription       string
	IntraStatArea              string
	IntraStatCountry           string
	IntraStatDeliveryTerm      string
	IntraStatTransactionA      string
	IntraStatTransactionB      string
	IntraStatTransportMethod   string
	LineNumber                 int32
	Notes                      string
	PrivateUsePercentage       float64
	Project                    string
	ProjectDescription         string
	Quantity                   float64
	SerialNumber               string
	StatisticalNetWeight       float64
	StatisticalNumber          string
	StatisticalQuantity        float64
	StatisticalValue           float64
	Subscription               string
	SubscriptionDescription    string
	To                         bigquery.NullTimestamp
	TrackingNumber             string
	TrackingNumberDescription  string
	Type                       int32
	VATAmountDC                float64
	VATAmountFC                float64
	VATBaseAmountDC            float64
	VATBaseAmountFC            float64
	VATCode                    string
	VATCodeDescription         string
	VATNonDeductiblePercentage float64
	VATPercentage              float64
	WithholdingAmountDC        float64
	WithholdingTax             string
}

func getPurchaseEntryLine(c *purchaseentry.PurchaseEntryLine, organisationID int64, softwareClientLicenceID int64) PurchaseEntryLine {
	return PurchaseEntryLine{
		organisationID,
		softwareClientLicenceID,
		c.ID.String(),
		c.AmountDC,
		c.AmountFC,
		c.Asset.String(),
		c.AssetDescription,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostUnit,
		c.CostUnitDescription,
		c.Description,
		c.Division,
		c.EntryID.String(),
		go_bigquery.DateToNullTimestamp(c.From),
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		c.IntraStatArea,
		c.IntraStatCountry,
		c.IntraStatDeliveryTerm,
		c.IntraStatTransactionA,
		c.IntraStatTransactionB,
		c.IntraStatTransportMethod,
		c.LineNumber,
		c.Notes,
		c.PrivateUsePercentage,
		c.Project.String(),
		c.ProjectDescription,
		c.Quantity,
		c.SerialNumber,
		c.StatisticalNetWeight,
		c.StatisticalNumber,
		c.StatisticalQuantity,
		c.StatisticalValue,
		c.Subscription.String(),
		c.SubscriptionDescription,
		go_bigquery.DateToNullTimestamp(c.To),
		c.TrackingNumber.String(),
		c.TrackingNumberDescription,
		c.Type,
		c.VATAmountDC,
		c.VATAmountFC,
		c.VATBaseAmountDC,
		c.VATBaseAmountFC,
		c.VATCode,
		c.VATCodeDescription,
		c.VATNonDeductiblePercentage,
		c.VATPercentage,
		c.WithholdingAmountDC,
		c.WithholdingTax,
	}
}

func (service *Service) WritePurchaseEntryLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.PurchaseEntryService().NewGetPurchaseEntryLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		purchaseEntryLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if purchaseEntryLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *purchaseEntryLines {
			batchRowCount++

			b, err := json.Marshal(getPurchaseEntryLine(&tl, organisationID, softwareClientLicenceID))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#PurchaseEntryLines flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#PurchaseEntryLines: %v\n", rowCount)

	return objectHandles, rowCount, PurchaseEntryLine{}, nil
}
