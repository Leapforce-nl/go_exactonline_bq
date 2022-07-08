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

type CRMContact struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	Timestamp                  int64
	Account                    string
	AccountIsCustomer          bool
	AccountIsSupplier          bool
	AccountMainContact         string
	AccountName                string
	AddressLine2               string
	AddressStreet              string
	AddressStreetNumber        string
	AddressStreetNumberSuffix  string
	AllowMailing               int32
	BirthDate                  bigquery.NullTimestamp
	BirthName                  string
	BirthNamePrefix            string
	BirthPlace                 string
	BusinessEmail              string
	BusinessFax                string
	BusinessMobile             string
	BusinessPhone              string
	BusinessPhoneExtension     string
	City                       string
	Code                       string
	Country                    string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Division                   int32
	Email                      string
	EndDate                    bigquery.NullTimestamp
	FirstName                  string
	FullName                   string
	Gender                     string
	HID                        int32
	ID                         string
	IdentificationDate         bigquery.NullTimestamp
	IdentificationDocument     string
	IdentificationUser         string
	Initials                   string
	IsAnonymised               byte
	IsMailingExcluded          bool
	IsMainContact              bool
	JobTitleDescription        string
	Language                   string
	LastName                   string
	LeadPurpose                string
	LeadSource                 string
	MarketingNotes             string
	MiddleName                 string
	Mobile                     string
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Nationality                string
	Notes                      string
	PartnerName                string
	PartnerNamePrefix          string
	Person                     string
	Phone                      string
	PhoneExtension             string
	PictureName                string
	PictureThumbnailURL        string
	PictureURL                 string
	Postcode                   string
	SocialSecurityNumber       string
	StartDate                  bigquery.NullTimestamp
	State                      string
	Title                      string
}

func getCRMContact(c *sync.CRMContact, softwareClientLicenseGuid string, maxTimestamp *int64) CRMContact {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return CRMContact{
		softwareClientLicenseGuid,
		t, t,
		timestamp,
		c.Account.String(),
		c.AccountIsCustomer,
		c.AccountIsSupplier,
		c.AccountMainContact.String(),
		c.AccountName,
		c.AddressLine2,
		c.AddressStreet,
		c.AddressStreetNumber,
		c.AddressStreetNumberSuffix,
		c.AllowMailing,
		go_bigquery.DateToNullTimestamp(c.BirthDate),
		c.BirthName,
		c.BirthNamePrefix,
		c.BirthPlace,
		c.BusinessEmail,
		c.BusinessFax,
		c.BusinessMobile,
		c.BusinessPhone,
		c.BusinessPhoneExtension,
		c.City,
		c.Code,
		c.Country,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.Email,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.FirstName,
		c.FullName,
		c.Gender,
		c.HID,
		c.ID.String(),
		go_bigquery.DateToNullTimestamp(c.IdentificationDate),
		c.IdentificationDocument.String(),
		c.IdentificationUser.String(),
		c.Initials,
		c.IsAnonymised,
		c.IsMailingExcluded,
		c.IsMainContact,
		c.JobTitleDescription,
		c.Language,
		c.LastName,
		c.LeadPurpose.String(),
		c.LeadSource.String(),
		c.MarketingNotes,
		c.MiddleName,
		c.Mobile,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Nationality,
		c.Notes,
		c.PartnerName,
		c.PartnerNamePrefix,
		c.Person.String(),
		c.Phone,
		c.PhoneExtension,
		c.PictureName,
		c.PictureThumbnailURL,
		c.PictureURL,
		c.Postcode,
		c.SocialSecurityNumber,
		go_bigquery.DateToNullTimestamp(c.StartDate),
		c.State,
		c.Title,
	}
}

func (service *Service) WriteCRMContacts(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncCRMContactsCall(&timestamp)

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

			b, err := json.Marshal(getCRMContact(&tl, softwareClientLicenseGuid, &maxTimestamp))
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

			fmt.Printf("#CRMContacts flushed: %v\n", batchRowCount)

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

	fmt.Printf("#CRMContacts: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
